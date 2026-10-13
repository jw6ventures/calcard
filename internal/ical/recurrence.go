// Package ical parses iCalendar content lines and expands recurrence rules
// (RRULE, RDATE, EXDATE, RECURRENCE-ID overrides). It has no knowledge of
// DAV or storage types; callers hand it raw iCalendar text and times.
package ical

import (
	"errors"
	"fmt"
	"math"
	"math/bits"
	"slices"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// BusyPeriod is one concrete busy interval of an event occurrence.
// RecurrenceID identifies its slot when the period came from a recurrence set.
// RecurrenceDate is the date that slot names when its series starts on a
// DATE, as RecurrenceInstance has it.
type BusyPeriod struct {
	RecurrenceID   time.Time
	RecurrenceDate time.Time
	Start          time.Time
	End            time.Time
}

// RecurrenceInstance is one generated occurrence of a recurrence set: the slot
// it occupies in the master's pattern, and where that occurrence actually
// falls. The two differ only when a RANGE=THISANDFUTURE override has moved it.
//
// The distinction is what RFC 5545 §3.8.4.4 needs: a RECURRENCE-ID names the
// slot rather than the moved time, so a caller writing one out cannot derive it
// from the occurrence's own DTSTART.
//
// An instance is identified and placed by its dates wherever it starts on a
// DATE, since a zone skipping a date resolves that date and the next to one
// instant: RecurrenceDate is the date its slot names when the series starts
// on a DATE, and StartDate and Days the date the occurrence starts on and the
// days it spans when what it carries starts on one. Each is zero otherwise.
type RecurrenceInstance struct {
	RecurrenceID   time.Time
	Start          time.Time
	End            time.Time
	RecurrenceDate time.Time
	StartDate      time.Time
	Days           int
}

// RecurrenceSlot is one identity in a master's recurrence pattern together
// with its original and effective placement. ExactOverride is true when a
// separate ordinary override replaces this slot. RecurrenceDate is the date
// the slot names when the series starts on a DATE, as RecurrenceInstance has
// it.
type RecurrenceSlot struct {
	RecurrenceID   time.Time
	RecurrenceDate time.Time
	Original       BusyPeriod
	Effective      BusyPeriod
	Suppressed     bool
	ExactOverride  bool
}

// recurrenceOccurrence is the internal form: the slot paired with the period
// the expansion actually produced for it.
type recurrenceOccurrence struct {
	recurrenceID  time.Time
	original      BusyPeriod
	period        BusyPeriod
	dates         occurrenceDays
	seriesOnDate  bool
	suppressed    bool
	exactOverride bool
}

// occurrenceDays is where an occurrence falls in dates: slot, the date its
// slot names -- a DATE's own, or a DATE-TIME's on the clock it is written in
// -- and, when what it carries starts on a DATE, start, the date it starts
// on, and days, the days it spans.
type occurrenceDays struct {
	slot  time.Time
	start time.Time
	days  int
}

// recurrenceDate is the date an occurrence's slot is identified by: its date
// in a series starting on a DATE, and none in a timed one.
func (o recurrenceOccurrence) recurrenceDate() time.Time {
	if !o.seriesOnDate {
		return time.Time{}
	}
	return o.dates.slot
}

// PropertyTimeResolver resolves one date-valued content line to the absolute
// instant it names. It exists so a caller holding a timezone this package cannot
// see -- RFC 4791 §7.3 gives a report the request's CALDAV:timezone, else the
// collection's CALDAV:calendar-timezone -- resolves every date in a recurrence
// set the same way it resolved the DTSTART it passes in. A nil resolver means
// ParsePropertyDateTimeLocal, which reads a floating value as UTC.
type PropertyTimeResolver func(keyPart, value string) (time.Time, bool)

func (resolve PropertyTimeResolver) or(keyPart, value string) (time.Time, bool) {
	if resolve == nil {
		return ParsePropertyDateTimeLocal(keyPart, value)
	}
	return resolve(keyPart, value)
}

// ResolvePropertyTime calls resolve, or ParsePropertyDateTimeLocal when it is
// nil.
func (resolve PropertyTimeResolver) ResolvePropertyTime(keyPart, value string) (time.Time, bool) {
	return resolve.or(keyPart, value)
}

// LocalTimeExists answers from the host's zone database, which is where
// ParsePropertyDateTimeLocal reads a TZID. A TZID the host does not define is
// read as UTC there, which skips nothing. A resolver reading a TZID through a
// definition of its own implements RecurrenceTimeResolver itself, so both
// answers come from one zone.
func (resolve PropertyTimeResolver) LocalTimeExists(tzid string, wall time.Time) bool {
	loc, ok := HostLocation(tzid)
	return !ok || WallClockExists(loc, wall)
}

// RecurrenceTimeResolver reads the date-valued content lines of a recurrence
// set in the zones the caller holds. ResolvePropertyTime is a
// PropertyTimeResolver. LocalTimeExists reports whether wall, a wall-clock
// reading held as the UTC time with the same fields, is a local time the zone
// tzid names actually has: RFC 5545 §3.3.10 leaves an instance the RRULE
// generates on a time a transition skips out of the set, uncounted, and only
// the definition ResolvePropertyTime reads the TZID through can say which times
// those are. A nil RecurrenceTimeResolver is PropertyTimeResolver(nil).
type RecurrenceTimeResolver interface {
	ResolvePropertyTime(keyPart, value string) (time.Time, bool)
	LocalTimeExists(tzid string, wall time.Time) bool
}

// propertyTimeResolver is the PropertyTimeResolver half of resolver.
func propertyTimeResolver(resolver RecurrenceTimeResolver) PropertyTimeResolver {
	if resolver == nil {
		return nil
	}
	return resolver.ResolvePropertyTime
}

// LocalTimeGaps lists the gaps of the host's zone tzid names, per
// LocalTimeGapLister. A TZID the host does not define skips nothing.
func (resolve PropertyTimeResolver) LocalTimeGaps(tzid string, after, through time.Time) ([]LocalTimeGap, bool) {
	loc, ok := HostLocation(tzid)
	if !ok {
		return nil, true
	}
	return hostLocalTimeGaps(loc)(after, through)
}

// LocalTimeGapLister is implemented by a RecurrenceTimeResolver that can list
// where a zone's local times are skipped, which lets a counted sub-daily rule
// be jumped over whole periods rather than generated one period at a time.
// LocalTimeGaps lists the gaps of the zone tzid names that start after after
// and at or before through, both wall clocks held as UTC fields; ok is false
// when the zone's definition cannot be followed that far. The resolver's
// LocalTimeExists must report true for every wall clock outside the gaps it
// lists: those are the only wall clocks a jump tests.
type LocalTimeGapLister interface {
	LocalTimeGaps(tzid string, after, through time.Time) (gaps []LocalTimeGap, ok bool)
}

// LocalDateReader is implemented by a RecurrenceTimeResolver that can read
// the date an instant falls on in a zone: the one the TZID tzid names, as
// ResolvePropertyTime reads it, or, for an empty tzid, the one a DATE and a
// floating value resolve in. A series starting on a DATE identifies each
// instance by its date, which a DATE-TIME RDATE of it has only on that zone's
// clock, and a timed series' slot falls on a date of its DTSTART's zone.
type LocalDateReader interface {
	LocalDate(tzid string, instant time.Time) time.Time
}

// localDateOf is the date instant falls on through resolver in the zone tzid
// names, as its UTC midnight; for a resolver that cannot say, in the host's
// zone of that name, else in UTC.
func localDateOf(resolver RecurrenceTimeResolver, tzid string, instant time.Time) time.Time {
	if reader, ok := resolver.(LocalDateReader); ok {
		return reader.LocalDate(tzid, instant)
	}
	if tzid != "" {
		if loc, ok := HostLocation(tzid); ok {
			return CivilDate(instant.In(loc))
		}
	}
	return CivilDate(instant.UTC())
}

// DateMidnightResolver is implemented by a RecurrenceTimeResolver that can
// place a date's midnight from the date itself. DateMidnight is the instant
// ResolvePropertyTime reads a DATE naming date as when its property carries
// the TZID tzid, empty for none, or date itself when it cannot be read; an
// expansion asks it for every date it places an all-day instance on.
type DateMidnightResolver interface {
	DateMidnight(tzid string, date time.Time) time.Time
}

// dateMidnights resolves dates to the instants their midnights fall on, each
// date once per key part: an expansion asks for the same dates over and over,
// one instance's end being the next one's start. direct places them when the
// resolver can.
type dateMidnights struct {
	resolve PropertyTimeResolver
	direct  DateMidnightResolver
	byKey   map[string]*keyMidnights
}

// keyMidnights are the midnights placed for one key part, whose TZID tzid is.
type keyMidnights struct {
	tzid  string
	dates map[int64]time.Time
}

func newDateMidnights(resolver RecurrenceTimeResolver) *dateMidnights {
	midnights := &dateMidnights{resolve: propertyTimeResolver(resolver)}
	midnights.direct, _ = resolver.(DateMidnightResolver)
	return midnights
}

// at is the midnight starting date as a DATE property with keyPart resolves
// it, or date's UTC midnight when it cannot be resolved.
func (d *dateMidnights) at(keyPart string, date time.Time) time.Time {
	placed, ok := d.byKey[keyPart]
	if !ok {
		placed = &keyMidnights{dates: make(map[int64]time.Time)}
		placed.tzid, _ = PropertyParam(keyPart, "TZID")
		if d.byKey == nil {
			d.byKey = make(map[string]*keyMidnights, 1)
		}
		d.byKey[keyPart] = placed
	}
	key := date.Unix()
	if instant, ok := placed.dates[key]; ok {
		return instant
	}
	var instant time.Time
	if d.direct != nil {
		instant = d.direct.DateMidnight(placed.tzid, date)
	} else if resolved, ok := d.resolve.or(keyPart, date.Format(DateLayout)); ok {
		instant = resolved
	} else {
		instant = date
	}
	placed.dates[key] = instant
	return instant
}

// localTimeExists is the existence half of resolver, defaulting as
// PropertyTimeResolver(nil) does.
func localTimeExists(resolver RecurrenceTimeResolver) func(tzid string, wall time.Time) bool {
	if resolver == nil {
		return PropertyTimeResolver(nil).LocalTimeExists
	}
	return resolver.LocalTimeExists
}

// localTimeGapsOf lists the gaps of the zone tzid names through resolver,
// defaulting as PropertyTimeResolver(nil) does. It is nil when resolver
// cannot list them.
func localTimeGapsOf(resolver RecurrenceTimeResolver, tzid string) localTimeGaps {
	var lister LocalTimeGapLister = PropertyTimeResolver(nil)
	if resolver != nil {
		var ok bool
		if lister, ok = resolver.(LocalTimeGapLister); !ok {
			return nil
		}
	}
	return func(after, through time.Time) ([]LocalTimeGap, bool) {
		return lister.LocalTimeGaps(tzid, after, through)
	}
}

// recurrenceExpansion selects which component the recurrence set belongs to,
// how its dates resolve, and how an occurrence is judged to reach the requested
// window. Consumers need different answers to the last: free-busy wants the
// half-open overlap a busy period has, time-range matching needs an inclusive
// candidate set for the stricter §9.9 condition, and recurrence limiting needs
// both the original and effective placements, including suppressed slots.
type recurrenceExpansion struct {
	componentName     string
	includeOverrides  bool
	includeSuppressed bool
	maxInstances      int
	resolve           RecurrenceTimeResolver
	reaches           func(original, effective BusyPeriod, suppressed bool, rangeStart, rangeEnd time.Time) bool
	// visit, when set, receives each occurrence in place of the returned
	// slice and ends the expansion by returning true.
	visit func(recurrenceOccurrence) bool
	// work, when set, is the budget the rule is walked on, which then
	// outlasts the expansion; a fresh one otherwise.
	work *recurrenceWork
	// walk, when set, records what the walk of the rule passes.
	walk *rruleWalk
}

// RecurringBusyPeriods expands a recurring event (RRULE, RDATE, EXDATE and
// RECURRENCE-ID overrides, including RANGE=THISANDFUTURE) into the concrete
// busy periods that overlap [rangeStart, rangeEnd). dtstart and duration are
// the event's resolved start and occurrence length; maxInstances caps the
// expansion and resolve reads the recurrence dates, per RecurrenceTimeResolver.
// A series starting on a DATE gives its length as whole days of 24 hours, and
// each instance spans that many local days from its own date (seriesLength);
// the same holds for VisitRecurrenceInstances and RecurrenceSlots.
//
// Exhausting maxInstances returns ErrRecurrenceExpansionLimit, and exhausting
// the rule's scan work returns ErrRecurrenceScanLimit, which wraps it. Either
// comes with the periods collected before the budget ran out. Each of those
// reaches the requested range, but together they are neither a prefix of the
// set nor in any particular order: rule-generated instances are collected
// before RDATEs and overrides, so an interrupted rule leaves those out
// entirely, and a RANGE=THISANDFUTURE override can move an instance past a
// later one. A caller that owes the complete set -- free-busy does, since a
// dropped period publishes an hour as free that is not -- has to treat the
// error as fatal rather than publish them.
func RecurringBusyPeriods(raw string, dtstart time.Time, duration time.Duration, rangeStart, rangeEnd time.Time, maxInstances int, resolve RecurrenceTimeResolver) ([]BusyPeriod, error) {
	occurrences, err := expandRecurrenceSet(raw, dtstart, duration, rangeStart, rangeEnd, recurrenceExpansion{
		componentName:    "VEVENT",
		includeOverrides: true,
		maxInstances:     maxInstances,
		resolve:          resolve,
		reaches: func(_ BusyPeriod, effective BusyPeriod, suppressed bool, rangeStart, rangeEnd time.Time) bool {
			return !suppressed && periodOverlaps(effective.Start, effective.End, rangeStart, rangeEnd)
		},
	})
	if err != nil && !errors.Is(err, ErrRecurrenceExpansionLimit) {
		return nil, err
	}
	periods := make([]BusyPeriod, 0, len(occurrences))
	for _, occurrence := range occurrences {
		period := occurrence.period
		period.RecurrenceID, period.RecurrenceDate = occurrence.recurrenceID, occurrence.recurrenceDate()
		periods = append(periods, period)
	}
	return periods, err
}

// VisitRecurrenceInstances expands the recurrence set of the named component
// and hands visit every *generated* instance whose occurrence window touches
// [rangeStart, rangeEnd], each paired with the slot of the master's pattern it
// belongs to, one at a time as the expansion produces them. Overridden instances
// are left out: RFC 4791 §9.7.1 scopes a comp-filter to each component of the
// resource, so an override is matched as the component it is rather than through
// its master's pattern. Both bounds are inclusive, because the caller re-applies
// the exact §9.9 condition and an exclusive bound here would drop an occurrence
// that starts precisely at the end of the range.
//
// The expansion ends as soon as visit returns true, which is not an error. A
// caller asking whether any one instance answers a question -- RFC 4791 §9.9
// makes one matching instance enough -- then pays for the instances up to that
// one rather than for the whole range.
//
// Work budget exhaustion returns ErrRecurrenceExpansionLimit (or the
// ErrRecurrenceScanLimit wrapping it) after visit has seen every instance
// collected before the budget ran out. Those are not a prefix of the set in
// ascending order -- RDATE instances are collected after the rule's, and a
// RANGE=THISANDFUTURE override can reorder placements -- but each of them does
// reach the range, so a caller asking only whether the set reaches it can
// answer from them; one that owes the complete set has to treat the error as
// fatal.
func VisitRecurrenceInstances(raw, componentName string, dtstart time.Time, duration time.Duration, rangeStart, rangeEnd time.Time, maxInstances int, resolve RecurrenceTimeResolver, visit func(RecurrenceInstance) bool) error {
	_, err := VisitRecurrenceInstancesAndSlots(raw, componentName, dtstart, duration, rangeStart, rangeEnd, maxInstances, resolve, nil, nil, visit)
	return err
}

// VisitRecurrenceInstancesAndSlots is VisitRecurrenceInstances, answering as
// well, for each of ids, whether the set generates the slot it names, whatever
// override then replaces or moves it: one its EXDATEs leave out, or a wall
// clock its zone skips, it does not. A slot is matched to an id by where it
// starts, never by how far its instance reaches, as the walk of the rule the
// range asks for passes it, so an id that walk passes costs nothing more.
//
// needed is called only for an id the walk did not pass -- before where the
// walk starts, for a rule it starts part way through, or past where it stops
// -- and returns true to have the id looked up; a nil needed looks every such
// id up. Each lookup is a walk of the rule on the work budget the range's walk
// was given: for a rule that walk starts part way through, a walk over the
// id's slot alone; otherwise one walk from the rule's start through the
// furthest id looked up, which only that budget bounds. An id not looked up is
// answered RecurrenceSlotNotLookedUp, and one still unanswered when the budget
// is spent RecurrenceSlotUnanswered.
func VisitRecurrenceInstancesAndSlots(raw, componentName string, dtstart time.Time, duration time.Duration, rangeStart, rangeEnd time.Time, maxInstances int, resolve RecurrenceTimeResolver, ids []RecurrenceSlotID, needed func(int) bool, visit func(RecurrenceInstance) bool) ([]RecurrenceSlotAnswer, error) {
	expansion := recurrenceExpansion{
		componentName:    componentName,
		includeOverrides: false,
		maxInstances:     maxInstances,
		resolve:          resolve,
		reaches: func(_ BusyPeriod, effective BusyPeriod, suppressed bool, rangeStart, rangeEnd time.Time) bool {
			return !suppressed && periodTouches(effective.Start, effective.End, rangeStart, rangeEnd)
		},
		visit: func(occurrence recurrenceOccurrence) bool {
			return visit(RecurrenceInstance{
				RecurrenceID:   occurrence.recurrenceID,
				Start:          occurrence.period.Start,
				End:            occurrence.period.End,
				RecurrenceDate: occurrence.recurrenceDate(),
				StartDate:      occurrence.dates.start,
				Days:           occurrence.dates.days,
			})
		},
	}
	set := readRecurrenceSet(raw, dtstart, duration, expansion)
	if len(ids) == 0 {
		_, err := set.expand(rangeStart, rangeEnd, expansion)
		return nil, err
	}
	query := newSlotQuery(ids)
	set.markListedSlots(query)
	walk := &rruleWalk{slots: query, fromFirst: true, toLast: true}
	expansion.work, expansion.walk = newRecurrenceWork(), walk
	_, err := set.expand(rangeStart, rangeEnd, expansion)
	set.lookUpUnpassedSlots(query, walk, needed, expansion)
	return query.answers, err
}

// RecurrenceSlots expands only the identities generated by the master. It
// retains suppressed slots and both placements so callers can decide which
// RANGE=THISANDFUTURE component impacts a requested window without reimplementing
// recurrence rules or override precedence. Work budget exhaustion returns
// ErrRecurrenceExpansionLimit.
func RecurrenceSlots(raw, componentName string, dtstart time.Time, duration time.Duration, rangeStart, rangeEnd time.Time, maxInstances int, resolve RecurrenceTimeResolver) ([]RecurrenceSlot, error) {
	occurrences, err := expandRecurrenceSet(raw, dtstart, duration, rangeStart, rangeEnd, recurrenceExpansion{
		componentName:     componentName,
		includeSuppressed: true,
		maxInstances:      maxInstances,
		resolve:           resolve,
		reaches: func(original, effective BusyPeriod, suppressed bool, rangeStart, rangeEnd time.Time) bool {
			return periodTouches(original.Start, original.End, rangeStart, rangeEnd) ||
				periodTouches(effective.Start, effective.End, rangeStart, rangeEnd)
		},
	})
	if err != nil {
		return nil, err
	}
	slots := make([]RecurrenceSlot, 0, len(occurrences))
	for _, occurrence := range occurrences {
		slots = append(slots, RecurrenceSlot{
			RecurrenceID:   occurrence.recurrenceID,
			RecurrenceDate: occurrence.recurrenceDate(),
			Original:       occurrence.original,
			Effective:      occurrence.period,
			Suppressed:     occurrence.suppressed,
			ExactOverride:  occurrence.exactOverride,
		})
	}
	return slots, nil
}

// RecurrenceIDNamesDate reports whether a RECURRENCE-ID names the slot it
// overrides by its date: a DATE naming a slot of a series whose DTSTART,
// seriesStart, is a DATE. Any other names the slot at the instant it resolves
// to. Recurrence expansion matches an override to its slot so, and a reader
// matching overrides to slots itself has to agree with it.
func RecurrenceIDNamesDate(seriesStart, recurrenceID PropertyValue) bool {
	return dateOnlyValue(seriesStart) && dateOnlyValue(recurrenceID)
}

// RecurrenceSlotID names a slot of a recurrence set as a RECURRENCE-ID does:
// at the instant it resolves to, and, for one naming it by date
// (RecurrenceIDNamesDate), by that date, held as its UTC midnight.
type RecurrenceSlotID struct {
	Instant time.Time
	Date    time.Time
}

// RecurrenceSlotAnswer is what VisitRecurrenceInstancesAndSlots found of one
// slot asked about.
type RecurrenceSlotAnswer uint8

const (
	// RecurrenceSlotNotLookedUp is a slot the walk of the range did not pass
	// and the caller did not have looked up: nothing is known of it.
	RecurrenceSlotNotLookedUp RecurrenceSlotAnswer = iota
	// RecurrenceSlotUnanswered is a slot looked up without an answer, the
	// work budget being spent first.
	RecurrenceSlotUnanswered
	// RecurrenceSlotGenerated is a slot the set generates.
	RecurrenceSlotGenerated
	// RecurrenceSlotNotGenerated is a slot the set never generates.
	RecurrenceSlotNotGenerated
)

// slotQuery holds the slots a caller asks whether a recurrence set generates,
// and the answer found for each.
type slotQuery struct {
	ids       []RecurrenceSlotID
	byInstant map[recurrenceInstant][]int
	byDate    map[int64][]int
	answers   []RecurrenceSlotAnswer
}

func newSlotQuery(ids []RecurrenceSlotID) *slotQuery {
	q := &slotQuery{
		ids:       ids,
		byInstant: make(map[recurrenceInstant][]int, len(ids)),
		answers:   make([]RecurrenceSlotAnswer, len(ids)),
	}
	for i, id := range ids {
		if !id.Date.IsZero() {
			if q.byDate == nil {
				q.byDate = make(map[int64][]int)
			}
			q.byDate[id.Date.Unix()] = append(q.byDate[id.Date.Unix()], i)
			continue
		}
		key := recurrenceInstant{seconds: id.Instant.Unix(), nanoseconds: id.Instant.Nanosecond()}
		q.byInstant[key] = append(q.byInstant[key], i)
	}
	return q
}

// mark records that the set generates the slot starting at instant, which a
// series starting on a DATE identifies by date; date is zero otherwise.
func (q *slotQuery) mark(instant, date time.Time) {
	if !date.IsZero() {
		for _, i := range q.byDate[date.Unix()] {
			q.answers[i] = RecurrenceSlotGenerated
		}
	}
	for _, i := range q.byInstant[recurrenceInstant{seconds: instant.Unix(), nanoseconds: instant.Nanosecond()}] {
		q.answers[i] = RecurrenceSlotGenerated
	}
}

// settle answers each slot still open that walk passed: the walk generated
// every slot it passed that the set has, so one left unmarked is not
// generated.
func (q *slotQuery) settle(walk *rruleWalk, open []int) {
	for _, i := range open {
		if q.answers[i] != RecurrenceSlotGenerated && walk.passed(q.ids[i]) {
			q.answers[i] = RecurrenceSlotNotGenerated
		}
	}
}

// rruleWalk is what one walk of a rule passed: every candidate from the
// rule's first, when fromFirst, else from the instant from on, through its
// last, when toLast, else through the instant through. slots, when set, has
// each candidate the walk generates marked.
type rruleWalk struct {
	slots     *slotQuery
	fromFirst bool
	from      time.Time
	toLast    bool
	through   time.Time
}

// passed reports whether the walk passed the slot id names. A slot named by
// date is matched by that date, which the series' midnight and the instant
// the id resolves to may place in different zones; a series starting on a
// DATE is walked on its wall clocks a day past the instants asked for, which
// passes the date wherever the walk passes that instant.
func (w *rruleWalk) passed(id RecurrenceSlotID) bool {
	return (w.fromFirst || !id.Instant.Before(w.from)) && (w.toLast || !id.Instant.After(w.through))
}

// markListedSlots marks the slots of q that the set lists rather than its
// rule generating them: its RDATEs and, when it has an RRULE or an RDATE to
// be a set at all, its DTSTART, each unless an EXDATE leaves it out.
func (set *recurrenceSet) markListedSlots(q *slotQuery) {
	if len(set.rdates) == 0 && componentPropertyValue(set.component, "RRULE") == "" {
		return
	}
	period, dates := set.length.instance(set.dtstart, set.startDate)
	set.markUnlessExcluded(q, period.Start, dates.slot)
	for _, rdate := range set.rdates {
		if set.length.onDate {
			set.markUnlessExcluded(q, set.length.span(rdate.Date, 1).Start, rdate.Date)
			continue
		}
		set.markUnlessExcluded(q, rdate.Start, rdate.Date)
	}
}

func (set *recurrenceSet) markUnlessExcluded(q *slotQuery, instant, date time.Time) {
	if set.exdates.excludes(instant, date) {
		return
	}
	if !set.length.onDate {
		date = time.Time{}
	}
	q.mark(instant, date)
}

// lookUpUnpassedSlots answers the slots of q the range's walk did not pass
// and needed, when set, has looked up, on the budget that walk was given. A
// rule the walk started part way through can be walked over one slot alone,
// and each slot is. Any other rule is walked from its start, which a walk per
// slot would repeat, so it is walked once, through the furthest slot looked
// up. A slot that walk leaves unanswered, the budget being spent, is
// RecurrenceSlotUnanswered.
func (set *recurrenceSet) lookUpUnpassedSlots(q *slotQuery, walk *rruleWalk, needed func(int) bool, expansion recurrenceExpansion) {
	var open []int
	for i, id := range q.ids {
		if q.answers[i] == RecurrenceSlotGenerated {
			continue
		}
		if walk.passed(id) {
			q.answers[i] = RecurrenceSlotNotGenerated
			continue
		}
		if needed == nil || needed(i) {
			open = append(open, i)
		}
	}
	if len(open) == 0 {
		return
	}
	rrule := componentPropertyValue(set.component, "RRULE")
	lookUp := func(from, through time.Time, slots []int) {
		lookup := expansion
		lookup.walk = &rruleWalk{slots: q}
		lookup.reaches = func(BusyPeriod, BusyPeriod, bool, time.Time, time.Time) bool { return false }
		// A walk the budget cuts short still answers the slots it passed.
		_, _ = rruleBusyPeriods(set.component, set.dtstart, set.length, rrule, set.exdates,
			from, through, lookup, func(time.Time, BusyPeriod, occurrenceDays) bool { return false })
		q.settle(lookup.walk, slots)
		for _, i := range slots {
			if q.answers[i] == RecurrenceSlotNotLookedUp {
				q.answers[i] = RecurrenceSlotUnanswered
			}
		}
	}
	if !walk.fromFirst {
		for _, i := range open {
			if q.answers[i] == RecurrenceSlotNotLookedUp {
				lookUp(q.ids[i].Instant, q.ids[i].Instant, []int{i})
			}
		}
		return
	}
	first, last := q.ids[open[0]].Instant, q.ids[open[0]].Instant
	for _, i := range open[1:] {
		first, last = minTime(first, q.ids[i].Instant), maxTime(last, q.ids[i].Instant)
	}
	lookUp(first, last, open)
}

func minTime(a, b time.Time) time.Time {
	if b.Before(a) {
		return b
	}
	return a
}

func maxTime(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

func expandRecurrenceSet(raw string, dtstart time.Time, duration time.Duration, rangeStart, rangeEnd time.Time, expansion recurrenceExpansion) ([]recurrenceOccurrence, error) {
	return readRecurrenceSet(raw, dtstart, duration, expansion).expand(rangeStart, rangeEnd, expansion)
}

// recurrenceSet is the recurrence set of one component of a resource, read
// once: its master, overrides, exclusions and RDATEs, which expand places
// over a range and can place over further ranges without reading the
// resource again.
type recurrenceSet struct {
	dtstart       time.Time
	duration      time.Duration
	component     *Component
	length        seriesLength
	exdates       exclusions
	overrides     []recurrenceOverride
	overrideIndex recurrenceOverrideIndex
	rdates        []rdatePeriod
	periodEnds    map[time.Time]time.Time
	startDate     time.Time
	// shiftSeconds and longerSeconds are how far the RANGE=THISANDFUTURE
	// overrides move an instance and lengthen it, which widen the scan of
	// the rule by as much.
	shiftSeconds  int64
	longerSeconds int64
}

// readRecurrenceSet reads the recurrence set of expansion's component of
// raw, whose dates expansion's resolver places.
func readRecurrenceSet(raw string, dtstart time.Time, duration time.Duration, expansion recurrenceExpansion) *recurrenceSet {
	// One unfold serves the master, the overrides and the RDATEs. This runs once
	// per candidate component of every resource a report considers, so a
	// per-reader scan of the payload would be paid that many times over.
	components := componentsNamedFromLines(UnfoldLines(raw), expansion.componentName)
	component := primaryOf(components)
	resolve := propertyTimeResolver(expansion.resolve)
	midnights := newDateMidnights(expansion.resolve)
	length := newSeriesLength(component, duration, midnights)
	exdates := eventExDates(component, resolve, length.onDate)
	overrides := componentRecurrenceOverrides(components, resolve, midnights, expansion.componentName, length.onDate, duration)
	overrideIndex := newRecurrenceOverrideIndex(overrides, length.onDate, midnights)
	rdates := componentRDatePeriods(component, resolve)
	periodEnds := make(map[time.Time]time.Time)
	// A RANGE=THISANDFUTURE override moving a timed series onto dates moves
	// each slot by the date it falls on in the zone of the series' DTSTART,
	// which an RDATE written in another form spells differently.
	var slotDate func(instant time.Time) time.Time
	if prop, ok := ComponentProperty(component, "DTSTART"); ok && !length.onDate && overrideIndex.thisAndFutureOnDate {
		tzid, _ := PropertyParam(prop.KeyPart, "TZID")
		utc := hasZoneSuffix(strings.TrimSpace(prop.Value))
		slotDate = func(instant time.Time) time.Time {
			if utc {
				return CivilDate(instant.UTC())
			}
			return localDateOf(expansion.resolve, strings.TrimSpace(tzid), instant)
		}
	}
	for i, rdate := range rdates {
		if length.onDate {
			// An instance of a DATE series is identified by its date, which a
			// DATE-TIME RDATE has on the clock of the zone DATEs resolve in,
			// and a PERIOD spans the days its hours reach into.
			if !rdate.onDate {
				rdates[i].Date = localDateOf(expansion.resolve, "", rdate.Start)
			}
			if !rdate.End.IsZero() {
				length.setPeriodDays(rdates[i].Date, DurationDays(rdate.End.Sub(rdate.Start)))
			}
			continue
		}
		if slotDate != nil {
			rdates[i].Date = slotDate(rdate.Start)
		}
		if !rdate.End.IsZero() {
			periodEnds[rdate.Start.UTC()] = rdate.End
		}
	}
	var startDate time.Time
	if prop, ok := ComponentProperty(component, "DTSTART"); ok {
		startDate, _ = DateOf(prop.Value)
	}
	return &recurrenceSet{
		dtstart: dtstart, duration: duration, component: component, length: length,
		exdates: exdates, overrides: overrides, overrideIndex: overrideIndex,
		rdates: rdates, periodEnds: periodEnds, startDate: startDate,
		shiftSeconds:  overrideIndex.maxThisAndFutureShiftSeconds(),
		longerSeconds: overrideIndex.maxThisAndFutureLengthSeconds() - int64(duration/time.Second),
	}
}

// expand is the occurrences of the set reaching [rangeStart, rangeEnd] as
// expansion judges them; its resolver is the one the set was read with.
func (set *recurrenceSet) expand(rangeStart, rangeEnd time.Time, expansion recurrenceExpansion) ([]recurrenceOccurrence, error) {
	dtstart, duration, component, length := set.dtstart, set.duration, set.component, set.length
	exdates, overrides, overrideIndex := set.exdates, set.overrides, set.overrideIndex
	rdates, periodEnds, startDate := set.rdates, set.periodEnds, set.startDate
	seen := make(map[occurrenceKey]struct{})
	// The slots of a timed series falling on one instant are one instance,
	// the first generated. A RANGE=THISANDFUTURE override starting on a DATE
	// moves each slot by the date it is written on, which two such slots
	// differ in where a floating time falls on a day its zone skips, so the
	// first is settled before the range is asked rather than by which of
	// them reaches it.
	var timedSlots recurrenceInstantSet
	if !length.onDate && overrideIndex.thisAndFutureOnDate {
		timedSlots = make(recurrenceInstantSet)
	}
	var periods []recurrenceOccurrence
	collected := 0
	var limitErr error
	stopped := false
	// recurrenceID is the slot the occurrence belongs to, which a
	// RANGE=THISANDFUTURE shift moves the period away from. EXDATE and an
	// ordinary RECURRENCE-ID override both name that slot, not the shifted
	// start, and in a series starting on a DATE they name it by its date.
	addPeriod := func(recurrenceID time.Time, original BusyPeriod, dates occurrenceDays, generated, applyExDates bool) {
		if limitErr != nil || stopped {
			return
		}
		if applyExDates && exdates.excludes(recurrenceID, dates.slot) {
			return
		}
		if generated && timedSlots != nil {
			if timedSlots.contains(recurrenceID) {
				return
			}
			timedSlots.add(recurrenceID)
		}
		if generated && !length.onDate {
			if end, ok := periodEnds[recurrenceID.UTC()]; ok {
				original.End = end
			}
		}
		effective, effectiveDates := original, dates
		suppressed := false
		exactOverride := false
		if generated {
			effective, effectiveDates, suppressed = overrideIndex.applyThisAndFuture(original, dates)
			exactOverride = overrideIndex.replaces(recurrenceID, dates.slot)
			suppressed = suppressed || exactOverride
		}
		if suppressed && !expansion.includeSuppressed {
			return
		}
		if !expansion.reaches(original, effective, suppressed, rangeStart, rangeEnd) {
			return
		}
		key := occurrenceKey{instant: recurrenceInstant{seconds: recurrenceID.Unix(), nanoseconds: recurrenceID.Nanosecond()}}
		if length.onDate && !dates.slot.IsZero() {
			key = occurrenceKey{instant: recurrenceInstant{seconds: dates.slot.Unix()}, onDate: true}
		}
		if _, ok := seen[key]; ok {
			return
		}
		if collected >= expansion.maxInstances {
			limitErr = ErrRecurrenceExpansionLimit
			return
		}
		seen[key] = struct{}{}
		collected++
		occurrence := recurrenceOccurrence{
			recurrenceID:  recurrenceID,
			original:      original,
			period:        effective,
			dates:         effectiveDates,
			seriesOnDate:  length.onDate,
			suppressed:    suppressed,
			exactOverride: exactOverride,
		}
		if expansion.visit == nil {
			periods = append(periods, occurrence)
		} else if expansion.visit(occurrence) {
			stopped = true
		}
	}
	// addStart adds the DTSTART as an instance, which it is whatever the rule
	// generates (RFC 5545 §3.8.5.3).
	addStart := func() {
		period, dates := length.instance(dtstart, startDate)
		addPeriod(period.Start, period, dates, true, true)
	}

	if rrule := componentPropertyValue(component, "RRULE"); rrule != "" {
		scanStart, scanEnd := rangeStart, rangeEnd
		if overrideIndex.hasThisAndFuture() {
			if shift := set.shiftSeconds; shift > 0 {
				scanStart = addSeconds(scanStart, -shift)
				scanEnd = addSeconds(scanEnd, shift)
			}
			// An instance the override lengthens reaches past where the
			// series' length would end it.
			if longer := set.longerSeconds; longer > 0 {
				scanStart = addSeconds(scanStart, -longer)
			}
			// An override starting on a DATE moves its instances by dates,
			// which a transition between an instance and where it lands
			// makes further than the hours between the override's own dates
			// measure, by as much as a day its zone skips or repeats.
			if overrideIndex.thisAndFutureOnDate {
				scanStart = scanStart.AddDate(0, 0, -1)
				scanEnd = scanEnd.AddDate(0, 0, 1)
			}
		}
		scanExpansion := expansion
		scanExpansion.reaches = func(_ BusyPeriod, effective BusyPeriod, _ bool, rangeStart, rangeEnd time.Time) bool {
			return periodTouches(effective.Start, effective.End, rangeStart, rangeEnd)
		}
		ok, err := rruleBusyPeriods(component, dtstart, length, rrule, exdates, scanStart, scanEnd, scanExpansion, func(id time.Time, period BusyPeriod, dates occurrenceDays) bool {
			addPeriod(id, period, dates, true, true)
			return limitErr != nil || stopped
		})
		if err != nil {
			// The occurrences already collected are kept for the same reason the
			// output budget keeps them: each one reaches the range, so a caller
			// asking whether the set reaches it can still answer. The RDATEs and
			// overrides below are never collected, which is why what is returned
			// is not a prefix of the set.
			return periods, err
		}
		if !ok {
			addStart()
		}
	} else if len(rdates) > 0 {
		addStart()
	}

	for _, rdate := range rdates {
		if length.onDate {
			days := length.days
			if !rdate.End.IsZero() {
				days = DurationDays(rdate.End.Sub(rdate.Start))
			}
			period := length.span(rdate.Date, days)
			addPeriod(period.Start, period, occurrenceDays{slot: rdate.Date, start: rdate.Date, days: days}, true, true)
			continue
		}
		end := rdate.End
		if end.IsZero() {
			end = rdate.Start.Add(duration)
		}
		addPeriod(rdate.Start, BusyPeriod{Start: rdate.Start, End: end}, occurrenceDays{slot: rdate.Date}, true, true)
	}

	if expansion.includeOverrides {
		for _, override := range overrides {
			if override.cancelled {
				continue
			}
			dates := occurrenceDays{start: override.startDate, days: override.days}
			if override.namesDate {
				dates.slot = override.recurrenceIDDate
			}
			addPeriod(override.recurrenceID, override.period, dates, false, false)
		}
	}

	// The periods collected before the budget ran out are returned with the
	// error rather than dropped. They are an arbitrary subset of the set rather
	// than a prefix, but every one of them reaches the range, so a caller testing
	// whether the set reaches it at all can answer from them; the callers that
	// owe a complete set discard them by refusing on the error.
	return periods, limitErr
}

type rdatePeriod struct {
	Start time.Time
	End   time.Time
	// Date is the date the RDATE's start names, as DateOf reads it, and onDate
	// reports a DATE value.
	Date   time.Time
	onDate bool
}

func EventHasRecurrence(ical string) bool {
	return componentHasRecurrence(ical, "VEVENT")
}

// componentHasRecurrence reports whether the named component's master carries a
// recurrence pattern this package can expand into instances.
func componentHasRecurrence(ical, componentName string) bool {
	component := PrimaryComponent(ical, componentName)
	return componentPropertyValue(component, "RRULE") != "" ||
		len(componentRDatePeriods(component, nil)) > 0
}

// SupportedRecurrenceRule reports whether an RRULE value uses a frequency this
// package expands. An unsupported one is not an error: the caller keeps the
// resource rather than silently filtering it out. An empty value is a component
// with no rule to expand, which is trivially supported.
func SupportedRecurrenceRule(rrule string) bool {
	if strings.TrimSpace(rrule) == "" {
		return true
	}
	freq, _ := RRuleParam(rrule, "FREQ")
	return supportedRecurrenceFreq(freq)
}

// ValidRecurrenceRule reports whether value uses the recurrence grammar this
// package can expand. It is intentionally independent of a DTSTART timezone;
// callers that expand the rule parse it again with the DTSTART location.
func ValidRecurrenceRule(value string) bool {
	_, ok := parseRecurrenceRule(value, time.UTC, nil, nil)
	return ok
}

// maxRecurrenceOverrideComponents bounds the RECURRENCE-ID components one
// resource may carry. A cancelled override adds no instance to the set, so
// CALDAV:max-instances does not bound them, yet every expansion of the resource
// reads each one; ten times the instance limit leaves room for a long-running
// series with many modified or cancelled occurrences.
const maxRecurrenceOverrideComponents = 10 * MaxRecurrenceInstances

// RecurrenceSetExceedsLimit counts the instance identities in the recurring
// VEVENT, VTODO, or VJOURNAL set carried by raw. The second result is false
// when a recurrence value cannot be parsed.
func RecurrenceSetExceedsLimit(raw string, limit int) (bool, bool) {
	return RecurrenceSetExceedsLimitFromUnfoldedLines(UnfoldLines(raw), limit)
}

// RecurrenceSetExceedsLimitFromUnfoldedLines is RecurrenceSetExceedsLimit for
// a caller that already unfolded the object's content lines.
func RecurrenceSetExceedsLimitFromUnfoldedLines(lines []string, limit int) (bool, bool) {
	return RecurrenceSetExceedsLimitOf(NewObjectTimes(lines), limit)
}

// RecurrenceSetExceedsLimitOf is RecurrenceSetExceedsLimit for the object times
// reads, resolving its dates as times does, as the DAV layer's matcher reads
// them.
func RecurrenceSetExceedsLimitOf(times *ObjectTimes, limit int) (bool, bool) {
	readTime := times.ResolvePropertyTime
	components := times.recurringComponents()
	if len(components) == 0 {
		return false, true
	}

	var master *Component
	// overrides maps each overridden slot to whether its override is
	// STATUS:CANCELLED. A cancelled override replaces the instance it names
	// rather than adding one -- only EXDATE removes an instance (RFC 5545
	// §3.8.5.1) -- so it is counted as the generated slot it names, and one
	// naming no generated slot adds nothing.
	overrides := make(map[string]bool)
	overrideComponents := 0
	instances := make(map[string]struct{})
	for i := range components {
		component := &components[i]
		recurrenceID, hasRecurrenceID := ComponentProperty(component, "RECURRENCE-ID")
		if !hasRecurrenceID {
			if master == nil {
				master = component
			}
			continue
		}
		parsed, ok := readTime(recurrenceID.KeyPart, recurrenceID.Value)
		if !ok {
			return false, false
		}
		overrideComponents++
		key := recurrenceInstantKey(parsed)
		cancelled := strings.EqualFold(componentPropertyValue(component, "STATUS"), "CANCELLED")
		if previous, seen := overrides[key]; !seen || previous {
			overrides[key] = cancelled
		}
		if !cancelled {
			instances["override:"+key] = struct{}{}
		}
	}
	if overrideComponents > maxRecurrenceOverrideComponents {
		return true, true
	}
	if master == nil {
		return len(instances) > limit, true
	}

	excluded := make(map[string]struct{})
	for _, property := range componentProperties(master, "EXDATE") {
		values, ok := recurrencePropertyInstants(property, readTime)
		if !ok {
			return false, false
		}
		for _, value := range values {
			excluded[recurrenceInstantKey(value)] = struct{}{}
		}
	}
	add := func(value time.Time) bool {
		key := recurrenceInstantKey(value)
		if _, skip := excluded[key]; skip {
			return false
		}
		if cancelled, replaced := overrides[key]; replaced && !cancelled {
			return false
		}
		instances["generated:"+key] = struct{}{}
		return len(instances) > limit
	}

	dtstartProperty, hasDTStart := ComponentProperty(master, "DTSTART")
	var dtstart time.Time
	var zone tzidDefinition
	// civil reports that the rule is followed on the wall clocks of the zone
	// the DTSTART names, held as UTC fields, as the expansion follows it. A
	// candidate on a local time that zone skips is then never generated or
	// counted, as RFC 5545 §3.3.10 requires, and the rest are counted as the
	// instants they resolve to, a repeated local time as its first occurrence
	// (§3.3.5).
	civil := false
	if hasDTStart {
		var ok bool
		dtstart, ok = readTime(dtstartProperty.KeyPart, dtstartProperty.Value)
		if !ok {
			return false, false
		}
		if tzid, zoned := localDateTimeTZID(dtstartProperty.KeyPart, dtstartProperty.Value); zoned {
			zone = times.zone(strings.TrimSpace(tzid))
			if zone.object != nil || zone.host != nil {
				civil = true
				dtstart, _ = ParseDateTime(strings.TrimSpace(dtstartProperty.Value))
			}
		}
	}
	instant := func(candidate time.Time) time.Time {
		if !civil {
			return candidate
		}
		return zone.instant(candidate)
	}

	hasRecurrence := false
	for _, property := range componentProperties(master, "RDATE") {
		hasRecurrence = true
		values, ok := recurrencePropertyInstants(property, readTime)
		if !ok {
			return false, false
		}
		for _, value := range values {
			if add(value) {
				return true, true
			}
		}
	}

	rrule := strings.TrimSpace(componentPropertyValue(master, "RRULE"))
	if rrule == "" {
		if hasDTStart {
			if add(instant(dtstart)) {
				return true, true
			}
		} else if !hasRecurrence {
			instances["master"] = struct{}{}
		}
		return len(instances) > limit, true
	}
	if !hasDTStart {
		return false, false
	}
	rule, ok := parseRecurrenceRule(rrule, dtstart.Location(), nil, dtstartWallClock(dtstartProperty.KeyPart, dtstartProperty.Value, readTime))
	if !ok {
		return false, false
	}
	pastUntil := func(resolved time.Time) bool {
		return rule.Until != nil && resolved.After(*rule.Until)
	}
	// untilWall bounds the wall clocks a rule ending at UNTIL can generate. A
	// wall clock is less than a day from the instant it names.
	var untilWall *time.Time
	if rule.Until != nil {
		bound := *rule.Until
		if civil {
			bound = bound.Add(recurrenceCivilScanPad)
		}
		untilWall = &bound
	}
	switch {
	case zone.object != nil:
		// A gap the definition cannot be followed to is not assumed: the
		// candidate is counted.
		rule.localTimeExists = existenceFromGaps(func(wall time.Time) bool {
			skipped, known := zone.skips(wall)
			return !known || !skipped
		}, zone.object)
	case civil:
		rule.localTimeExists = existenceFromGaps(func(wall time.Time) bool {
			return WallClockExists(zone.host, wall)
		}, hostLocalTimeGaps(zone.host))
	}
	if add(instant(dtstart)) {
		return true, true
	}
	if !recurrenceBySetPosMaySelect(dtstart, rule) {
		return len(instances) > limit, true
	}
	work := newRecurrenceWork()
	generatesStart, overBudget := recurrenceRuleGeneratesStart(dtstart, rule, work)
	if overBudget {
		return true, true
	}
	// countLimit is how many instances the rule itself contributes toward
	// COUNT once the DTSTART, counted above, has taken its place.
	countLimit := rule.Count
	if !generatesStart && rule.Count > 0 {
		if rule.Count == 1 {
			return len(instances) > limit, true
		}
		countLimit--
	}

	// A rule with neither COUNT nor UNTIL never stops, so the limit cannot be a
	// count of the whole set without refusing every "repeats weekly, no end
	// date" event -- the default shape of every major client. It is measured
	// instead over one year from DTSTART, which admits those rules and still
	// refuses the sub-daily ones whose expansion is genuinely expensive.
	// Expansion at read time is bounded separately, by the requested range.
	unbounded := rule.Count == 0 && rule.Until == nil
	horizon := dtstart.AddDate(unboundedRecurrenceHorizonYears, 0, 0)

	periodStart := recurrencePeriodStart(dtstart, rule)
	occurrences := 0
	barren := newBarrenPeriodRun(rule)
	for scanned := 0; scanned < recurrenceScanLimit; scanned++ {
		bounded, exceeded, offered := false, false, false
		_, overBudget := visitRecurrenceCandidates(periodStart, dtstart, rule, work, func(current time.Time) bool {
			offered = true
			if current.Before(dtstart) {
				return false
			}
			resolved := instant(current)
			if pastUntil(resolved) {
				bounded = true
				return true
			}
			if unbounded && current.After(horizon) {
				bounded = true
				return true
			}
			occurrences++
			if countLimit > 0 && occurrences > countLimit {
				bounded = true
				return true
			}
			if add(resolved) {
				exceeded = true
				return true
			}
			return false
		})
		// A period too large to generate is a period too large to be inside the
		// advertised instance limit, so it answers the question on its own.
		if overBudget || exceeded {
			return true, true
		}
		if bounded || barren.settles(offered) {
			return len(instances) > limit, true
		}
		next := advanceRecurrencePeriod(periodStart, rule)
		if !next.After(periodStart) {
			return len(instances) > limit, true
		}
		periodStart = next
		earliest := recurrencePeriodEarliestCandidate(periodStart, rule)
		if untilWall != nil && earliest.After(*untilWall) {
			return len(instances) > limit, true
		}
		if unbounded && earliest.After(horizon) {
			return len(instances) > limit, true
		}
	}
	mayProduceCandidate := occurrences > 0 || recurrenceRuleMayProduceCandidate(dtstart, rule)
	if rule.Count > 0 && mayProduceCandidate {
		if rule.Count-len(excluded) > limit {
			return true, true
		}
	}
	if !mayProduceCandidate {
		return len(instances) > limit, true
	}
	countedRule := rule
	countedRule.Count = countLimit
	countedRule.Until = untilWall
	addCandidate := func(candidate time.Time) bool {
		resolved := instant(candidate)
		return !pastUntil(resolved) && add(resolved)
	}
	if exceeds, handled := finishSparseRecurrence(periodStart, dtstart, countedRule, work, &occurrences, addCandidate); handled {
		return exceeds, true
	}
	// The scan guard ran out before the horizon above was reached, so how many
	// instances this rule generates in a year is still unknown. Only a rule
	// dense enough to spend 100000 periods inside one year gets here, and for
	// those the conservative answer is the right one.
	if unbounded {
		return true, true
	}
	return len(instances) > limit, true
}

// recurrenceRuleMayProduceCandidate distinguishes a merely sparse bounded rule
// from one whose filters can never select a recurrence period. Gregorian dates
// repeat every 400 years. Reducing the recurrence interval against that cycle
// also accounts for sub-daily clock alignment without visiting every second in
// the cycle. Which local times a zone skips is not considered: a daily rule
// generating only skipped times is followed until the range or the work budget
// ends, and running out of the budget fails closed.
func recurrenceRuleMayProduceCandidate(dtstart time.Time, rule recurrenceRule) bool {
	switch rule.Freq {
	case "SECONDLY", "MINUTELY", "HOURLY", "DAILY":
	default:
		// The normal scan covers at least a complete calendar cycle of periods
		// for weekly and coarser frequencies. No candidate there means the rule
		// cannot justify treating COUNT as an attainable lower bound.
		return false
	}
	if !recurrenceBySetPosMaySelect(dtstart, rule) {
		return false
	}
	base := recurrencePeriodStart(dtstart, rule)

	var subDailyResidues map[int64]struct{}
	var subDailyDivisor int64
	if _, subDaily := recurrenceFrequencySeconds(rule.Freq); subDaily {
		stepSeconds, ok := subDailyRecurrenceSeconds(rule)
		if !ok {
			return false
		}
		subDailyDivisor = greatestCommonDivisor(stepSeconds, daysInGregorianCycle*24*60*60)
		subDailyResidues = make(map[int64]struct{})
		for _, offset := range allowedSubDailyPeriodOffsets(rule) {
			subDailyResidues[positiveRemainder(offset, subDailyDivisor)] = struct{}{}
		}
		if len(subDailyResidues) == 0 {
			return false
		}
	}

	days := rule.days()
	dailyDivisor := greatestCommonDivisor(int64(rule.Interval), daysInGregorianCycle)
	for dayNumber := recurrenceDayMaskOrigin; dayNumber < recurrenceDayMaskOrigin+daysInGregorianCycle; dayNumber++ {
		if !days.admits(dayNumber) {
			continue
		}
		if rule.Freq == "DAILY" {
			if positiveRemainder(dayNumber-civilDayNumber(base), dailyDivisor) == 0 {
				return true
			}
			continue
		}
		required := positiveRemainder(base.Unix()-civilDayStartUnix(dayNumber, dtstart.Location()), subDailyDivisor)
		if _, ok := subDailyResidues[required]; ok {
			return true
		}
	}
	return false
}

// daysInGregorianCycle is the length of the 400-year cycle after which the
// Gregorian calendar repeats, weekdays included: it is exactly 20,871 weeks.
const daysInGregorianCycle = int64(146097)

// recurrenceDayMask marks the days of one Gregorian cycle, starting on
// recurrenceDayMaskOrigin, that a rule's date filters admit. Those filters read
// only the calendar date, which repeats with the cycle, so the mask answers for
// every day there is, and a scan that steps from one admitted day to the next
// reads a word for every 64 days it passes over rather than looking at each.
type recurrenceDayMask struct {
	words     []uint64
	admitsAny bool
}

// recurrenceDayMaskOrigin is the civil day number of 2000-01-01, the first day
// a recurrenceDayMask describes.
var recurrenceDayMaskOrigin = civilDayNumber(time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC))

func newRecurrenceDayMask(rule recurrenceRule) recurrenceDayMask {
	mask := recurrenceDayMask{words: make([]uint64, (daysInGregorianCycle+63)/64)}
	index := 0
	weekday := civilWeekday(2000, time.January, 1)
	for year := 2000; year < 2400; year++ {
		yearDay := 0
		for month := time.January; month <= time.December; month++ {
			for monthDay := 1; monthDay <= daysInMonth(year, month); monthDay++ {
				yearDay++
				if civilDateMatchesRule(year, month, monthDay, yearDay, weekday, rule) {
					mask.words[index/64] |= 1 << (index % 64)
					mask.admitsAny = true
				}
				index++
				weekday = (weekday + 1) % 7
			}
		}
	}
	return mask
}

// lazyRecurrenceDayMask is a rule's recurrenceDayMask, built on first use.
type lazyRecurrenceDayMask struct {
	once sync.Once
	mask recurrenceDayMask
}

// days is the rule's recurrenceDayMask, built once per parsed rule however many
// readers of the rule ask for it.
func (rule recurrenceRule) days() recurrenceDayMask {
	if rule.dayMask == nil {
		return newRecurrenceDayMask(rule)
	}
	rule.dayMask.once.Do(func() { rule.dayMask.mask = newRecurrenceDayMask(rule) })
	return rule.dayMask.mask
}

// admits reports whether the day with the given civil day number is admitted.
func (m recurrenceDayMask) admits(dayNumber int64) bool {
	index := positiveRemainder(dayNumber-recurrenceDayMaskOrigin, daysInGregorianCycle)
	return m.words[index/64]&(1<<(index%64)) != 0
}

// next returns the civil day number of the first admitted day at or after from,
// and how many words of the mask it read to find it, which is never more than
// one cycle's worth. ok is false when the mask admits no day at all.
func (m recurrenceDayMask) next(from int64) (dayNumber int64, wordsRead int, ok bool) {
	if !m.admitsAny {
		return 0, 0, false
	}
	index := positiveRemainder(from-recurrenceDayMaskOrigin, daysInGregorianCycle)
	dayNumber = from
	for {
		wordsRead++
		if word := m.words[index/64] >> (index % 64); word != 0 {
			return dayNumber + int64(bits.TrailingZeros64(word)), wordsRead, true
		}
		advance := min(64-index%64, daysInGregorianCycle-index)
		dayNumber += advance
		index = (index + advance) % daysInGregorianCycle
	}
}

// civilDayStart is the start of the day with the given civil day number in loc.
func civilDayStart(dayNumber int64, loc *time.Location) time.Time {
	return time.Date(2000, time.January, 1+int(dayNumber-recurrenceDayMaskOrigin), 0, 0, 0, 0, loc)
}

// civilDayStartUnix is civilDayStart(dayNumber, loc).Unix(), counted in whole
// days in UTC, where every day is 86,400 seconds long.
func civilDayStartUnix(dayNumber int64, loc *time.Location) int64 {
	if loc == time.UTC {
		return (dayNumber - unixEpochDayNumber) * 24 * 60 * 60
	}
	return civilDayStart(dayNumber, loc).Unix()
}

// unixEpochDayNumber is the civil day number of 1970-01-01.
var unixEpochDayNumber = civilDayNumber(time.Unix(0, 0).UTC())

// barrenPeriodRun recognizes a daily or coarser rule that will never offer
// another candidate. What a period offers depends on where it falls in the
// Gregorian cycle, and a rule's periods step through every position they can
// take there in a fixed number of periods: at most 400 for a yearly rule and
// 4,800 for a monthly one. A run of that many periods offering nothing has seen
// every position the rule will ever reach, so no later period can offer
// anything either. Time-zone transitions are the one thing two periods at the
// same position can differ in -- in where a period falls, and in which of its
// local times exist to be offered -- and past a zone's recorded history they
// repeat by a yearly rule as well.
//
// Without this a rule whose BY parts can never hold together is scanned to the
// period limit, which for a yearly rule naming hundreds of days a year is
// minutes of work for an answer one cycle already gave. Sub-daily rules are
// left to the sparse scan, which skips the days they cannot match.
type barrenPeriodRun struct {
	cycle int
	run   int
}

func newBarrenPeriodRun(rule recurrenceRule) barrenPeriodRun {
	var units int64
	switch rule.Freq {
	case "DAILY":
		units = daysInGregorianCycle
	case "WEEKLY":
		units = daysInGregorianCycle / 7
	case "MONTHLY":
		units = 400 * 12
	case "YEARLY":
		units = 400
	default:
		return barrenPeriodRun{}
	}
	interval := int64(max(rule.Interval, 1))
	return barrenPeriodRun{cycle: int(units / greatestCommonDivisor(interval, units))}
}

// settles records whether the latest period offered any candidate, and reports
// that the periods since the last one that did make up a whole cycle.
func (r *barrenPeriodRun) settles(offered bool) bool {
	if offered || r.cycle == 0 {
		r.run = 0
		return false
	}
	r.run++
	return r.run >= r.cycle
}

func recurrenceFrequencySeconds(freq string) (int64, bool) {
	switch freq {
	case "SECONDLY":
		return 1, true
	case "MINUTELY":
		return 60, true
	case "HOURLY":
		return 60 * 60, true
	default:
		return 0, false
	}
}

func allowedSubDailyPeriodOffsets(rule recurrenceRule) []int64 {
	var offsets []int64
	for hour := 0; hour < 24; hour++ {
		if !intMatchesIfPresent(hour, rule.ByHour) {
			continue
		}
		if rule.Freq == "HOURLY" {
			offsets = append(offsets, int64(hour*60*60))
			continue
		}
		for minute := 0; minute < 60; minute++ {
			if !intMatchesIfPresent(minute, rule.ByMinute) {
				continue
			}
			if rule.Freq == "MINUTELY" {
				offsets = append(offsets, int64((hour*60+minute)*60))
				continue
			}
			for second := 0; second < 60; second++ {
				if intMatchesIfPresent(second, rule.BySecond) || (second == 59 && intInSlice(60, rule.BySecond)) {
					offsets = append(offsets, int64((hour*60+minute)*60+second))
				}
			}
		}
	}
	return offsets
}

func greatestCommonDivisor(a, b int64) int64 {
	if a < 0 {
		a = -a
	}
	if b < 0 {
		b = -b
	}
	for b != 0 {
		a, b = b, a%b
	}
	return a
}

func positiveRemainder(value, divisor int64) int64 {
	result := value % divisor
	if result < 0 {
		result += divisor
	}
	return result
}

// recurrenceBySetPosMaySelect reports whether the rule can select anything at
// all. RFC 5545 §3.3.10 numbers BYSETPOS within one period, so an ordinal
// larger than any period of the rule can ever offer selects nothing however
// many periods are generated -- and generating them to find that out costs
// seconds of CPU for a two-hundred byte object, on the validation gate and
// again on every expansion of the stored resource.
func recurrenceBySetPosMaySelect(dtstart time.Time, rule recurrenceRule) bool {
	maxCandidates := recurrenceMaxCandidatesPerPeriod(dtstart, rule)
	if maxCandidates == 0 {
		return false
	}
	if len(rule.BySetPos) == 0 {
		return true
	}
	for _, position := range rule.BySetPos {
		if position >= -maxCandidates && position <= maxCandidates {
			return true
		}
	}
	return false
}

// recurrenceCandidateBoundCeiling saturates the candidate-count arithmetic
// below. Every BYSETPOS value parses within ±366, so any bound at or above that
// answers identically and the exact figure past it carries no information.
const recurrenceCandidateBoundCeiling = 1 << 20

// recurrenceMaxCandidatesPerPeriod is an upper bound on the candidate starts
// one period of the rule offers, derived from the rule rather than by
// generating a period. It mirrors visitRecurrenceCandidates: the days the
// frequency's day list can hold, times the clock values each of those days
// expands to. Over-estimating only makes the BYSETPOS guard weaker, so each
// part is the widest count its generator can produce.
func recurrenceMaxCandidatesPerPeriod(dtstart time.Time, rule recurrenceRule) int {
	seconds := validSecondCandidateCount(rule.BySecond)
	minutes := len(defaultedInts(rule.ByMinute, dtstart.Minute()))
	switch rule.Freq {
	case "SECONDLY":
		// A second is one instant, so the period offers itself or nothing.
		return 1
	case "MINUTELY":
		return seconds
	case "HOURLY":
		return saturatingProduct(minutes, seconds)
	}
	hours := len(defaultedInts(rule.ByHour, dtstart.Hour()))
	perDay := saturatingProduct(saturatingProduct(hours, minutes), seconds)
	return saturatingProduct(recurrenceMaxDaysPerPeriod(dtstart, rule), perDay)
}

// recurrenceMaxDaysPerPeriod is an upper bound on the entries the day list of
// one period can hold, after the deduplication by calendar date that
// visitRecurrenceCandidates applies to it.
func recurrenceMaxDaysPerPeriod(dtstart time.Time, rule recurrenceRule) int {
	switch rule.Freq {
	case "DAILY":
		return 1
	case "WEEKLY":
		if len(rule.ByDay) == 0 {
			return 1
		}
		return weekdayDayCountBound(rule.ByDay, 7)
	case "MONTHLY":
		return monthlyDayCountBound(rule.ByMonthDay, rule.ByDay)
	case "YEARLY":
		return yearlyDayCountBound(rule)
	default:
		return 1
	}
}

// monthlyDayCountBound bounds monthlyDays. Each BYMONTHDAY value names at most
// one date of a month, and a BYDAY list is bounded over the longest month.
func monthlyDayCountBound(byMonthDay []int, byDay []weekdaySpecifier) int {
	if len(byMonthDay) > 0 {
		return len(byMonthDay)
	}
	if len(byDay) > 0 {
		return weekdayDayCountBound(byDay, 31)
	}
	return 1
}

// yearlyDayCountBound bounds yearlyDays, following the same branches it takes.
func yearlyDayCountBound(rule recurrenceRule) int {
	if len(rule.ByWeekNo) > 0 {
		perWeek := 7
		// A whole-week list is narrowed afterwards by an unnumbered BYDAY, which
		// selects the same weekdays out of every one of those weeks.
		if len(rule.ByDay) > 0 && !hasOrdinalWeekday(rule.ByDay) {
			perWeek = weekdayDayCountBound(rule.ByDay, 7)
		}
		return saturatingProduct(len(rule.ByWeekNo), perWeek)
	}
	if len(rule.ByYearDay) > 0 {
		return len(rule.ByYearDay)
	}
	if len(rule.ByMonth) == 0 && len(rule.ByDay) > 0 && len(rule.ByMonthDay) == 0 {
		// The whole year is searched, whether the days are taken from it
		// directly or month by month.
		return weekdayDayCountBound(rule.ByDay, 366)
	}
	months := len(rule.ByMonth)
	if months == 0 {
		if len(rule.ByMonthDay) > 0 {
			months = 12
		} else {
			months = 1
		}
	}
	return saturatingProduct(months, monthlyDayCountBound(rule.ByMonthDay, rule.ByDay))
}

// weekdayDayCountBound bounds the dates a BYDAY list names inside a period
// spanning at most periodDays days. A numbered specifier names one date.
//
// The unnumbered ones are counted together rather than one at a time, because
// they share the period's days: a stretch of periodDays holds exactly
// periodDays/7 complete weeks, so each named weekday occurs that often, and
// only the periodDays%7 days left over can add one more apiece. Summing a
// per-weekday maximum instead would claim 25 weekdays in a 31-day month, which
// no month has, and hand a BYSETPOS the scan then has to disprove by
// generating every period of the rule.
func weekdayDayCountBound(specifiers []weekdaySpecifier, periodDays int) int {
	numbered := 0
	var unnumbered [7]bool
	distinct := 0
	for _, spec := range specifiers {
		if spec.Ordinal != 0 {
			numbered++
			continue
		}
		if unnumbered[spec.Day] {
			continue
		}
		unnumbered[spec.Day] = true
		distinct++
	}
	total := numbered
	if distinct > 0 {
		total = saturatingSum(total, distinct*(periodDays/7)+min(distinct, periodDays%7))
	}
	return total
}

func saturatingProduct(a, b int) int {
	if a <= 0 || b <= 0 {
		return 0
	}
	if a > recurrenceCandidateBoundCeiling/b {
		return recurrenceCandidateBoundCeiling
	}
	return a * b
}

func saturatingSum(a, b int) int {
	if a > recurrenceCandidateBoundCeiling-b {
		return recurrenceCandidateBoundCeiling
	}
	return a + b
}

func validSecondCandidateCount(values []int) int {
	if len(values) == 0 {
		return 1
	}
	candidates := make(map[int]struct{}, len(values))
	for _, value := range values {
		if value >= 0 && value <= 60 {
			if value == 60 {
				value = 59
			}
			candidates[value] = struct{}{}
		}
	}
	return len(candidates)
}

// ErrRecurrenceExpansionLimit means a recurrence could not be fully evaluated
// within the work budget.
var ErrRecurrenceExpansionLimit = errors.New("recurrence expansion work limit exceeded")

// ErrRecurrenceScanLimit is the ErrRecurrenceExpansionLimit raised when a rule
// needs more candidate generation than the scan budgets allow, as opposed to
// generating more instances than the caller's output budget holds. It is a
// property of the stored rule over the range rather than of how many instances
// the range contains, and wraps ErrRecurrenceExpansionLimit so a caller that
// treats every incomplete expansion alike need not tell the two apart.
var ErrRecurrenceScanLimit = fmt.Errorf("%w: recurrence rule scan work exhausted", ErrRecurrenceExpansionLimit)

// finishSparseRecurrence carries on a sub-daily or daily rule the period scan
// gave up on, one admitted day at a time, while work remains. Running out of work fails
// closed on max-instances.
func finishSparseRecurrence(periodStart, dtstart time.Time, rule recurrenceRule, work *recurrenceWork, occurrences *int, add func(time.Time) bool) (bool, bool) {
	if rule.Until == nil && (rule.Count == 0 || *occurrences == 0) {
		return false, false
	}
	switch rule.Freq {
	case "SECONDLY", "MINUTELY", "HOURLY", "DAILY":
	default:
		return false, false
	}

	var until time.Time
	if rule.Until != nil {
		until = *rule.Until
	}
	exceeds := false
	complete := false
	addCandidate := func(current time.Time) bool {
		if rule.Until != nil && current.After(until) {
			return false
		}
		if rule.Count > 0 {
			if *occurrences >= rule.Count {
				complete = true
				return true
			}
			(*occurrences)++
		}
		if add(current) {
			exceeds = true
			complete = true
			return true
		}
		if rule.Count > 0 && *occurrences >= rule.Count {
			complete = true
			return true
		}
		return false
	}

	stopped, overBudget := visitSparseRecurrence(periodStart, dtstart, until, rule, work, addCandidate)
	if overBudget {
		return true, true
	}
	if stopped {
		return exceeds, complete
	}
	return false, true
}

// visitSparseRecurrence follows a daily or sub-daily rule from periodStart
// through the days its date filters admit, so how far apart those days fall
// costs a mask word per 64 days rather than a unit per day.
func visitSparseRecurrence(periodStart, dtstart, until time.Time, rule recurrenceRule, work *recurrenceWork, add func(time.Time) bool) (stopped, overBudget bool) {
	days := rule.days()
	for from := civilDayNumber(periodStart); ; {
		dayNumber, wordsRead, ok := days.next(from)
		if !ok {
			return false, false
		}
		if !work.spend(wordsRead) {
			return true, true
		}
		from = dayNumber + 1
		day := civilDayStart(dayNumber, dtstart.Location())
		if !until.IsZero() && day.After(until) {
			return false, false
		}
		if rule.Freq == "DAILY" {
			if day.Before(periodStart) || !dailyRecurrencePeriodAligned(day, dtstart, rule.Interval) {
				continue
			}
			stopped, overBudget := visitRecurrenceCandidates(day, dtstart, rule, work, func(current time.Time) bool {
				if current.Before(dtstart) || (!until.IsZero() && current.After(until)) {
					return false
				}
				return add(current)
			})
			// A day over budget leaves its candidates only partly visited, so
			// it is failed closed rather than treated as producing none, the
			// same way every other visitRecurrenceCandidates caller handles it.
			if overBudget {
				return true, true
			}
			if stopped {
				return true, false
			}
			continue
		}

		if stop, overBudget := visitSparseSubDailyPeriods(day, periodStart, dtstart, until, rule, work, add); stop {
			return true, overBudget
		}
	}
}

func visitSparseSubDailyPeriods(day, periodStart, dtstart, until time.Time, rule recurrenceRule, work *recurrenceWork, add func(time.Time) bool) (stop, overBudget bool) {
	stepSeconds, ok := subDailyRecurrenceSeconds(rule)
	if !ok {
		return false, false
	}
	base := recurrencePeriodStart(dtstart, rule)
	dayEnd := day.AddDate(0, 0, 1)
	firstUnix := day.Unix()
	if periodStart.After(day) {
		firstUnix = periodStart.Unix()
	}
	remainder := positiveRemainder(firstUnix-base.Unix(), stepSeconds)
	if remainder != 0 {
		firstUnix += stepSeconds - remainder
	}
	lastUnix := dayEnd.Unix()
	if !until.IsZero() && until.Before(dayEnd) {
		lastUnix = until.Unix() + 1
	}

	for periodUnix := firstUnix; periodUnix < lastUnix; {
		period := time.Unix(periodUnix, 0).In(day.Location())
		stopped, periodOverBudget := visitRecurrenceCandidates(period, dtstart, rule, work, func(current time.Time) bool {
			if current.Before(dtstart) || (!until.IsZero() && current.After(until)) {
				return false
			}
			return add(current)
		})
		if periodOverBudget {
			return true, true
		}
		if stopped {
			return true, false
		}
		if periodUnix > int64(^uint64(0)>>1)-stepSeconds {
			break
		}
		periodUnix += stepSeconds
	}
	return false, false
}

func dailyRecurrencePeriodAligned(day, dtstart time.Time, interval int) bool {
	if interval <= 0 {
		return false
	}
	base := recurrencePeriodStart(dtstart, recurrenceRule{Freq: "DAILY"})
	delta := civilDayNumber(day) - civilDayNumber(base)
	return delta >= 0 && delta%int64(interval) == 0
}

func civilDayNumber(value time.Time) int64 {
	year := int64(value.Year())
	month := int64(value.Month())
	if month <= 2 {
		year--
	}
	era := year / 400
	yearOfEra := year - era*400
	adjustedMonth := month - 3
	if adjustedMonth < 0 {
		adjustedMonth += 12
	}
	dayOfYear := (153*adjustedMonth+2)/5 + int64(value.Day()) - 1
	dayOfEra := yearOfEra*365 + yearOfEra/4 - yearOfEra/100 + dayOfYear
	return era*146097 + dayOfEra
}

func recurrencePropertyInstants(property PropertyValue, read PropertyTimeResolver) ([]time.Time, bool) {
	parts := strings.Split(property.Value, ",")
	values := make([]time.Time, 0, len(parts))
	for _, part := range parts {
		start := strings.TrimSpace(part)
		if slash := strings.IndexByte(start, '/'); slash >= 0 {
			start = strings.TrimSpace(start[:slash])
		}
		parsed, ok := read.or(property.KeyPart, start)
		if !ok {
			return nil, false
		}
		values = append(values, parsed)
	}
	return values, true
}

func recurrenceInstantKey(value time.Time) string {
	return value.UTC().Format(time.RFC3339Nano)
}

// LatestRecurrenceOnOrBefore returns the latest occurrence of the recurrence
// set -- the DTSTART or a start the rule generates -- at or before the supplied
// wall-clock value. It is used to apply the observance rules in a submitted
// VTIMEZONE definition.
//
// found reports that such a start exists. complete reports that the search
// reached the end of what the rule describes; it is false when a period was too
// large to generate within recurrencePeriodWorkLimit, or when either scan bound
// ran out. A latest returned alongside complete == false is at best the maximum
// over the part that was searched, which is not the rule's, so the caller has to
// refuse the value rather than apply it -- an observance chosen from a
// half-generated period names the wrong UTC offset, and nothing downstream can
// tell.
func LatestRecurrenceOnOrBefore(dtstart, before time.Time, rrule string) (latest time.Time, found, complete bool) {
	return latestRecurrenceOnOrBefore(dtstart, before, rrule, newRecurrenceWork())
}

// latestRecurrenceOnOrBefore is LatestRecurrenceOnOrBefore drawing on work.
func latestRecurrenceOnOrBefore(dtstart, before time.Time, rrule string, work *recurrenceWork) (latest time.Time, found, complete bool) {
	rule, ok := parseRecurrenceRule(rrule, dtstart.Location(), nil, wallClockIn(dtstart.Location()))
	if !ok || before.Before(dtstart) {
		return time.Time{}, false, true
	}
	// RFC 5545 §3.8.5.3 makes the DTSTART the first occurrence whether or not
	// the rule generates it, and whether or not it falls past UNTIL; a
	// VTIMEZONE observance's DTSTART is its first onset (§3.6.5). A rule that
	// selects nothing leaves it the only one.
	if !recurrenceBySetPosMaySelect(dtstart, rule) {
		return dtstart, true, true
	}
	generatesStart, overBudget := recurrenceRuleGeneratesStart(dtstart, rule, work)
	if overBudget {
		return dtstart, true, false
	}
	pastUntil := func(candidate time.Time) bool {
		return rule.Until != nil && candidate.After(*rule.Until) && !candidate.Equal(dtstart)
	}
	threshold := before
	if rule.Until != nil && rule.Until.Before(threshold) {
		threshold = *rule.Until
	}

	if rule.Count > 0 {
		// COUNT numbers occurrences from the DTSTART, so a counted rule can only
		// be followed forward from there.
		lister := newRecurrenceStartLister(dtstart, rule, generatesStart, work)
		lister.latestOnly = true
		// An incomplete listing holds the latest start reached, which may lie
		// past what it covers.
		complete := lister.ListThrough(before, math.MaxInt)
		if len(lister.starts) == 0 {
			return time.Time{}, false, complete
		}
		return lister.starts[len(lister.starts)-1], true, complete
	}

	// The DTSTART answers only when no generated occurrence does, since every
	// generated one is at or after it.
	fallback := func() (time.Time, bool) {
		if generatesStart {
			return time.Time{}, false
		}
		return dtstart, true
	}
	firstPeriod := recurrencePeriodStart(dtstart, rule)
	periodStart := fastForwardRecurrencePeriod(firstPeriod, threshold, rule)
	// The period after the one holding the threshold can already have
	// generated starts at or before it, so the search begins there.
	if next := advanceRecurrencePeriod(periodStart, rule); next.After(periodStart) && !recurrencePeriodEarliestCandidate(next, rule).After(threshold) {
		periodStart = next
	}
	// A thousand periods back is further than any observance rule a time zone
	// uses needs; a rule that offers nothing needs a whole cycle of periods to
	// show it.
	barren := newBarrenPeriodRun(rule)
	for scanned := 0; scanned < max(1000, barren.cycle); scanned++ {
		// The whole period is wanted here, so a period too large to generate
		// leaves no usable answer: without BYSETPOS the maximum reached is only
		// the maximum of the days that were generated, and with it nothing is
		// selected at all, because BYSETPOS resolves only once the period ends.
		offered := false
		_, overBudget := visitRecurrenceCandidates(periodStart, dtstart, rule, work, func(candidate time.Time) bool {
			offered = true
			if candidate.Before(dtstart) || candidate.After(before) || pastUntil(candidate) {
				return false
			}
			if latest.IsZero() || candidate.After(latest) {
				latest = candidate
			}
			return false
		})
		if overBudget {
			return latest, !latest.IsZero(), false
		}
		if !latest.IsZero() {
			return latest, true, true
		}
		// The period holding the DTSTART usually begins before it, and the
		// rule can still generate starts after the DTSTART within it.
		previous := retreatRecurrencePeriod(periodStart, rule)
		if !previous.Before(periodStart) || previous.Before(firstPeriod) || barren.settles(offered) {
			start, found := fallback()
			return start, found, true
		}
		periodStart = previous
	}
	if !recurrenceRuleMayProduceCandidate(dtstart, rule) {
		start, found := fallback()
		return start, found, true
	}
	return time.Time{}, false, false
}

// RecurrenceStartLister lists, in ascending order, the starts of the recurrence
// set a DTSTART and RRULE describe: the DTSTART, which RFC 5545 §3.8.5.3 makes
// the first instance whether or not the rule generates it, and every start the
// rule generates after it. It is the set LatestRecurrenceOnOrBefore searches,
// listed once, so a caller resolving many instants against one VTIMEZONE
// observance can look each one up rather than walk the rule again per instant.
//
// The listing grows on demand, and each extension continues from where the last
// one stopped rather than from the DTSTART. Every extension draws on one work
// budget, the one a single LatestRecurrenceOnOrBefore spends, so growing a
// listing in steps cannot carry it further than one walk of the rule may go. A
// lister is not safe for concurrent use.
type RecurrenceStartLister struct {
	dtstart time.Time
	rule    recurrenceRule
	starts  []time.Time
	// covered is the instant through which starts holds every start.
	covered time.Time
	// ended reports that starts holds the whole set.
	ended bool
	// halted reports that the listing cannot be extended past covered: the
	// rule needed more work than this package undertakes, or the set held
	// more starts than the caller's limit.
	halted bool
	// latestOnly keeps only the latest start listed, for a caller that asks
	// nothing else of the listing and may follow a rule through hundreds of
	// thousands of starts to reach it.
	latestOnly bool
	// keepFrom, when set, is where the listing starts keeping starts: those
	// before it are followed but not kept, save the latest, dropped.
	keepFrom   time.Time
	dropped    time.Time
	hasDropped bool
	work       *recurrenceWork

	periodStart time.Time
	occurrences int
	barren      barrenPeriodRun
	scanned     int
}

// NewRecurrenceStartLister prepares a listing of the set dtstart and rrule
// describe. Nothing past the DTSTART is generated until ListThrough asks.
func NewRecurrenceStartLister(dtstart time.Time, rrule string) *RecurrenceStartLister {
	rule, ok := parseRecurrenceRule(rrule, dtstart.Location(), nil, wallClockIn(dtstart.Location()))
	if !ok {
		// LatestRecurrenceOnOrBefore finds nothing in a rule it cannot parse,
		// not even the DTSTART.
		return &RecurrenceStartLister{dtstart: dtstart, ended: true}
	}
	if !recurrenceBySetPosMaySelect(dtstart, rule) {
		return &RecurrenceStartLister{dtstart: dtstart, starts: []time.Time{dtstart}, ended: true}
	}
	work := newRecurrenceWork()
	generatesStart, overBudget := recurrenceRuleGeneratesStart(dtstart, rule, work)
	if overBudget {
		return &RecurrenceStartLister{dtstart: dtstart, halted: true}
	}
	return newRecurrenceStartLister(dtstart, rule, generatesStart, work)
}

// NewRecurrenceStartListerFrom prepares a listing of the same set that keeps
// only the starts at or after from, and the latest start before it, so it
// answers LatestOnOrBefore for every bound at or after from, and for none
// before it. The rule is followed from its DTSTART, drawing on the listing's
// work budget, but the starts before from are neither kept nor counted against
// ListThrough's limit, so a rule too dense to list from its DTSTART within
// that limit is listed around the instants asked about.
func NewRecurrenceStartListerFrom(dtstart time.Time, rrule string, from time.Time) *RecurrenceStartLister {
	lister := NewRecurrenceStartLister(dtstart, rrule)
	lister.keepFrom = from
	lister.dropBefore(len(lister.starts))
	return lister
}

// recurrenceStartsBetween lists, in ascending order, the starts the rule rrule
// generates from dtstart after after and at or before through, leaving out the
// DTSTART, which the set holds whether or not the rule generates it. They are
// the starts LatestRecurrenceOnOrBefore would answer, asked again from through
// back to after, but the rule is followed once, forward, on work: a rule
// without COUNT from the period holding after, a counted one from its DTSTART,
// since COUNT numbers its occurrences from there. ok is false when the starts
// number more than limit, or the rule cannot be followed to through on what
// work has left.
func recurrenceStartsBetween(dtstart time.Time, rrule string, after, through time.Time, limit int, work *recurrenceWork) (starts []time.Time, ok bool) {
	rule, parsed := parseRecurrenceRule(rrule, dtstart.Location(), nil, wallClockIn(dtstart.Location()))
	if !parsed || !recurrenceBySetPosMaySelect(dtstart, rule) {
		return nil, true
	}
	if after.Before(dtstart) {
		after = dtstart
	}
	// A rule retired by UNTIL before the span generates nothing in it, and
	// following it there would step every period since UNTIL.
	if !through.After(after) || (rule.Until != nil && !rule.Until.After(after)) {
		return nil, true
	}
	var lister *RecurrenceStartLister
	if rule.Count > 0 || after.Equal(dtstart) {
		generatesStart, overBudget := recurrenceRuleGeneratesStart(dtstart, rule, work)
		if overBudget {
			return nil, false
		}
		lister = newRecurrenceStartLister(dtstart, rule, generatesStart, work)
		lister.keepFrom = after.Add(time.Nanosecond)
		lister.dropBefore(len(lister.starts))
	} else {
		firstPeriod := recurrencePeriodStart(dtstart, rule)
		lister = &RecurrenceStartLister{
			dtstart:     dtstart,
			rule:        rule,
			work:        work,
			covered:     after,
			periodStart: rewindToPeriodReaching(fastForwardRecurrencePeriod(firstPeriod, after, rule), firstPeriod, after, rule),
			barren:      newBarrenPeriodRun(rule),
		}
	}
	if !lister.ListThrough(through, limit) {
		return nil, false
	}
	return lister.starts, true
}

// dropBefore moves the first n starts, those before keepFrom, out of the
// listing, keeping the latest of them as dropped.
func (l *RecurrenceStartLister) dropBefore(n int) {
	kept := sort.Search(min(n, len(l.starts)), func(i int) bool { return !l.starts[i].Before(l.keepFrom) })
	if kept > 0 {
		l.dropped, l.hasDropped = l.starts[kept-1], true
		l.starts = l.starts[kept:]
	}
}

func newRecurrenceStartLister(dtstart time.Time, rule recurrenceRule, generatesStart bool, work *recurrenceWork) *RecurrenceStartLister {
	lister := &RecurrenceStartLister{
		dtstart:     dtstart,
		rule:        rule,
		work:        work,
		periodStart: recurrencePeriodStart(dtstart, rule),
		barren:      newBarrenPeriodRun(rule),
	}
	if !generatesStart {
		lister.starts = []time.Time{dtstart}
		lister.occurrences = 1
	}
	return lister
}

// Len is the number of starts listed so far.
func (l *RecurrenceStartLister) Len() int {
	return len(l.starts)
}

// Covers reports whether the listing holds every start at or before through.
func (l *RecurrenceStartLister) Covers(through time.Time) bool {
	return l.ended || !through.After(l.covered)
}

// LatestOnOrBefore answers what LatestRecurrenceOnOrBefore answers for the same
// bound, from the listing. ok is false when the listing does not reach before,
// or, for a listing kept from an instant, before falls before that instant.
func (l *RecurrenceStartLister) LatestOnOrBefore(before time.Time) (latest time.Time, found, ok bool) {
	if before.Before(l.keepFrom) {
		return time.Time{}, false, false
	}
	if before.Before(l.dtstart) {
		return time.Time{}, false, true
	}
	if !l.Covers(before) {
		return time.Time{}, false, false
	}
	index, exact := slices.BinarySearchFunc(l.starts, before, time.Time.Compare)
	if exact {
		index++
	}
	if index == 0 {
		if l.hasDropped {
			return l.dropped, true, true
		}
		return time.Time{}, false, true
	}
	return l.starts[index-1], true, true
}

// StartsBetween lists the starts recurrenceStartsBetween lists for the same
// span -- every start after after and at or before through but the DTSTART --
// from the listing. ok is false when the listing does not hold all of them:
// it does not reach through, or, kept from an instant, that instant falls
// after after. The caller must not modify starts. They stay valid while the
// listing grows: a listing only ever appends past its length, and a halt drops
// only starts past what it covered, none of which a span it covered can hold.
func (l *RecurrenceStartLister) StartsBetween(after, through time.Time) (starts []time.Time, ok bool) {
	if after.Before(l.dtstart) {
		after = l.dtstart
	}
	if !through.After(after) {
		return nil, true
	}
	if !l.Covers(through) || after.Before(l.keepFrom) {
		return nil, false
	}
	first := sort.Search(len(l.starts), func(i int) bool { return l.starts[i].After(after) })
	end := sort.Search(len(l.starts), func(i int) bool { return l.starts[i].After(through) })
	return l.starts[first:end:end], true
}

// ListThrough extends the listing to hold every start at or before through,
// keeping at most limit starts in all, and reports whether it now does. A
// listing that halts keeps what it listed exactly, but is not extended again:
// the work or the starts it would take have already been found to be too many.
func (l *RecurrenceStartLister) ListThrough(through time.Time, limit int) bool {
	if through.Before(l.dtstart) {
		through = l.dtstart
	}
	if l.Covers(through) {
		return true
	}
	if l.halted {
		return false
	}
	for ; l.scanned < recurrenceScanLimit; l.scanned++ {
		if l.rule.Count > 0 && l.occurrences >= l.rule.Count {
			l.ended = true
			return true
		}
		if recurrencePeriodEarliestCandidate(l.periodStart, l.rule).After(through) {
			l.covered = through
			return true
		}
		offered, finished, passed := false, false, false
		var overflow time.Time
		_, overBudget := visitRecurrenceCandidates(l.periodStart, l.dtstart, l.rule, l.work, func(candidate time.Time) bool {
			offered = true
			// A period the last extension stopped partway through is visited
			// again from its start, and what it listed then is skipped.
			if candidate.Before(l.dtstart) || !candidate.After(l.covered) {
				return false
			}
			if l.rule.Until != nil && candidate.After(*l.rule.Until) && !candidate.Equal(l.dtstart) {
				finished = true
				return true
			}
			if candidate.After(through) {
				passed = true
				return true
			}
			if l.rule.Count > 0 && l.occurrences >= l.rule.Count {
				finished = true
				return true
			}
			if candidate.Before(l.keepFrom) {
				l.occurrences++
				l.dropped, l.hasDropped = candidate, true
				return false
			}
			if len(l.starts) >= limit {
				overflow = candidate
				return true
			}
			l.occurrences++
			if l.latestOnly && len(l.starts) > 0 {
				l.starts[0] = candidate
			} else {
				l.starts = append(l.starts, candidate)
			}
			return false
		})
		switch {
		case overBudget:
			// Only the periods before this one were visited whole.
			l.halt(l.periodStart.Add(-time.Nanosecond))
			return false
		case !overflow.IsZero():
			l.halt(overflow.Add(-time.Nanosecond))
			return false
		case finished:
			l.ended = true
			return true
		case passed:
			l.covered = through
			return true
		case l.barren.settles(offered):
			l.ended = true
			return true
		}
		next := advanceRecurrencePeriod(l.periodStart, l.rule)
		if !next.After(l.periodStart) {
			l.halt(l.periodStart.Add(-time.Nanosecond))
			return false
		}
		l.periodStart = next
	}
	// The scan bound ran out. Unless the rule can never produce a candidate,
	// what was reached is not the end of it.
	if !recurrenceRuleMayProduceCandidate(l.dtstart, l.rule) {
		l.ended = true
		return true
	}
	l.halt(l.periodStart.Add(-time.Nanosecond))
	return false
}

// halt stops the listing, which holds every start at or before covered, and
// drops whatever it listed past that. A latestOnly listing keeps its one start,
// the latest reached, which LatestRecurrenceOnOrBefore answers with when the
// listing is incomplete.
func (l *RecurrenceStartLister) halt(covered time.Time) {
	if covered.After(l.covered) {
		l.covered = covered
	}
	l.halted = true
	if l.latestOnly {
		return
	}
	kept := sort.Search(len(l.starts), func(i int) bool { return l.starts[i].After(l.covered) })
	l.starts = l.starts[:kept]
}

func retreatRecurrencePeriod(periodStart time.Time, rule recurrenceRule) time.Time {
	interval := rule.Interval
	if interval <= 0 {
		interval = 1
	}
	switch rule.Freq {
	case "SECONDLY":
		return periodStart.Add(-time.Duration(interval) * time.Second)
	case "MINUTELY":
		return periodStart.Add(-time.Duration(interval) * time.Minute)
	case "HOURLY":
		return periodStart.Add(-time.Duration(interval) * time.Hour)
	case "DAILY":
		return periodStart.AddDate(0, 0, -interval)
	case "WEEKLY":
		return periodStart.AddDate(0, 0, -7*interval)
	case "MONTHLY":
		return periodStart.AddDate(0, -interval, 0)
	case "YEARLY":
		return periodStart.AddDate(-interval, 0, 0)
	default:
		return periodStart
	}
}

// recurrenceCivilScanPad bounds how far a civil-time scan window is widened
// past the absolute request bounds, since a valid UTC offset is strictly less
// than 24 hours.
const recurrenceCivilScanPad = 24 * time.Hour

func rruleBusyPeriods(component *Component, dtstart time.Time, length seriesLength, rrule string, exdates exclusions, rangeStart, rangeEnd time.Time, expansion recurrenceExpansion, add func(time.Time, BusyPeriod, occurrenceDays) bool) (bool, error) {
	duration := length.duration
	resolve := propertyTimeResolver(expansion.resolve)
	recurrenceStart, resolveCandidate, candidateExists, gaps, civil := recurrenceGenerationStart(component, dtstart, expansion.resolve)
	var startWall wallClockResolver
	if property, ok := ComponentProperty(component, "DTSTART"); ok {
		startWall = dtstartWallClock(property.KeyPart, property.Value, resolve)
	}
	// walk records what is passed: nothing yet, and every candidate when the
	// rule is ended or cannot be read, generating nothing past the DTSTART.
	walk := expansion.walk
	if walk == nil {
		walk = &rruleWalk{}
	}
	*walk = rruleWalk{slots: walk.slots, fromFirst: true}
	rule, ok := parseRecurrenceRule(rrule, recurrenceStart.Location(), resolve, startWall)
	if !ok {
		walk.toLast = true
		return false, nil
	}
	rule.localTimeExists = candidateExists
	if candidateExists != nil && gaps != nil {
		rule.localTimeExists = existenceFromGaps(candidateExists, gaps)
	}

	occurrences := 0
	// done ends the walk, when add asks it to or, ended, when the rule has no
	// candidate left.
	done := false
	ended := func() bool {
		done, walk.toLast = true, true
		return true
	}
	visit := func(current time.Time) bool {
		if current.Before(recurrenceStart) {
			return false
		}
		// A DATE candidate is its date, placed through the expansion's
		// midnights rather than resolved afresh.
		var period BusyPeriod
		var dates occurrenceDays
		if length.onDate {
			period, dates = length.instance(current, CivilDate(current))
		} else {
			resolvedCurrent, resolved := resolveCandidate(current)
			if !resolved {
				return false
			}
			period = BusyPeriod{Start: resolvedCurrent, End: resolvedCurrent.Add(duration)}
			dates = occurrenceDays{slot: CivilDate(current)}
		}
		resolvedCurrent := period.Start
		// The DTSTART is an instance even past UNTIL (RFC 5545 §3.8.5.3).
		if rule.Until != nil && resolvedCurrent.After(*rule.Until) && !current.Equal(recurrenceStart) {
			return ended()
		}
		occurrences++
		if rule.Count > 0 && occurrences > rule.Count {
			return ended()
		}
		// resolvedCurrent is the slot the rule generated, which is matched
		// to the slots asked about by where it starts; the shared expansion
		// applies any RANGE=THISANDFUTURE transform afterwards.
		if walk.slots != nil && !exdates.excludes(resolvedCurrent, dates.slot) {
			slotDate := dates.slot
			if !length.onDate {
				slotDate = time.Time{}
			}
			walk.slots.mark(resolvedCurrent, slotDate)
		}
		// Every candidate up to this one has been passed. A civil walk
		// passes wall clocks in order but not the instants they resolve to,
		// which a transition can move a day either way of the wall clock.
		passed := resolvedCurrent
		if civil {
			passed = current.Add(-recurrenceCivilScanPad)
		}
		if passed.After(walk.through) {
			walk.through = passed
		}
		if expansion.reaches(period, period, false, rangeStart, rangeEnd) && !exdates.excludes(resolvedCurrent, dates.slot) {
			if add(resolvedCurrent, period, dates) {
				done = true
				return true
			}
		}
		return false
	}
	// passedRange records a walk that went on past rangeEnd without being
	// ended: every candidate up to it was passed, whatever its wall clock.
	passedRange := func() {
		if !done && rangeEnd.After(walk.through) {
			walk.through = rangeEnd
		}
	}

	// RFC 5545 §3.8.5.3 makes the DTSTART the first instance of the set, and
	// the first toward COUNT, whether or not the rule generates it. A rule that
	// does generate it visits it below like any other candidate.
	work := expansion.work
	if work == nil {
		work = newRecurrenceWork()
	}
	selects := recurrenceBySetPosMaySelect(recurrenceStart, rule)
	generatesStart := false
	if selects {
		var overBudget bool
		generatesStart, overBudget = recurrenceRuleGeneratesStart(recurrenceStart, rule, work)
		if overBudget {
			// Whether or not the rule generates it, the DTSTART is an
			// instance, so it is collected with whatever else is returned.
			visit(recurrenceStart)
			return false, ErrRecurrenceScanLimit
		}
	}
	if !generatesStart {
		visit(recurrenceStart)
		if done {
			return true, nil
		}
	}
	// A rule whose BYSETPOS names an ordinal no period of it can reach generates
	// nothing past the DTSTART. That is an answer, and one the rule gives on its
	// own: scanning for it would burn the whole work budget on every report the
	// resource appears in and still end with nothing to return.
	if !selects {
		walk.toLast = true
		return true, nil
	}

	scanStart, scanEnd := rangeStart, rangeEnd
	if civil {
		// A valid UTC offset is strictly less than 24 hours. Padding the absolute
		// request bounds by that amount produces a safe civil-time scan window;
		// every candidate is still resolved and checked against the exact bounds.
		scanStart = scanStart.Add(-recurrenceCivilScanPad)
		scanEnd = scanEnd.Add(recurrenceCivilScanPad)
	}
	// A walk started part way through the rule passes every candidate from
	// one occurrence before rangeStart on: a civil walk starts a day of wall
	// clock earlier still, and a wall clock is less than a day from the
	// instant it names.
	periodStart := recurrencePeriodStart(recurrenceStart, rule)
	if rule.Count == 0 {
		threshold := scanStart.Add(-duration)
		periodStart = rewindToPeriodReaching(fastForwardRecurrencePeriod(periodStart, threshold, rule), periodStart, threshold, rule)
		walk.fromFirst, walk.from = false, rangeStart.Add(-duration)
	} else if fastForwardedStart, skipped, ok := fastForwardCountedSubDailyRecurrence(periodStart, scanStart.Add(-duration), recurrenceStart, rule, rule.Count-occurrences, gaps); ok {
		walk.fromFirst, walk.from = false, rangeStart.Add(-duration)
		if occurrences+skipped >= rule.Count {
			walk.toLast = true
			return true, nil
		}
		periodStart = fastForwardedStart
		occurrences += skipped
	}
	barren := newBarrenPeriodRun(rule)
	for scanned := 0; scanned < recurrenceScanLimit; scanned++ {
		offered := false
		_, overBudget := visitRecurrenceCandidates(periodStart, recurrenceStart, rule, work, func(current time.Time) bool {
			offered = true
			return visit(current)
		})
		if overBudget {
			return false, ErrRecurrenceScanLimit
		}
		if done {
			return true, nil
		}
		if barren.settles(offered) {
			walk.toLast = true
			return true, nil
		}
		next := advanceRecurrencePeriod(periodStart, rule)
		if !next.After(periodStart) {
			return false, ErrRecurrenceScanLimit
		}
		periodStart = next
		if rule.Count > 0 && occurrences >= rule.Count {
			walk.toLast = true
			return true, nil
		}
		if recurrencePeriodEarliestCandidate(periodStart, rule).After(scanEnd) {
			passedRange()
			return true, nil
		}
	}
	switch rule.Freq {
	case "SECONDLY", "MINUTELY", "HOURLY", "DAILY":
		// A daily rule's cycle runs to more periods than the scan above
		// covers, so a rule that can never produce a candidate is recognized
		// from the rule itself, before following it day by day to scanEnd.
		if !recurrenceRuleMayProduceCandidate(recurrenceStart, rule) {
			walk.toLast = true
			return true, nil
		}
		_, overBudget := visitSparseRecurrence(periodStart, recurrenceStart, scanEnd, rule, work, visit)
		if !overBudget {
			passedRange()
			return true, nil
		}
	}
	return false, ErrRecurrenceScanLimit
}

// recurrenceRuleGeneratesStart reports whether rule itself generates dtstart,
// which only the period containing it can. overBudget reports that the period
// was too large to decide within recurrencePeriodWorkLimit.
func recurrenceRuleGeneratesStart(dtstart time.Time, rule recurrenceRule, work *recurrenceWork) (generates, overBudget bool) {
	// A rule with no BY part derives every candidate from the DTSTART itself.
	if !recurrenceRuleHasOtherByPart(rule) && len(rule.BySetPos) == 0 {
		return true, false
	}
	_, overBudget = visitRecurrenceCandidates(recurrencePeriodStart(dtstart, rule), dtstart, rule, work, func(candidate time.Time) bool {
		if candidate.Before(dtstart) {
			return false
		}
		generates = candidate.Equal(dtstart)
		return true
	})
	return generates && !overBudget, overBudget
}

// recurrenceGenerationStart is where a rule's candidates are generated from
// and how each one becomes an instance. A DTSTART with no zone suffix is a wall
// clock, so its candidates are generated on wall clocks, held as UTC fields, and
// each is resolved through resolver the way the DTSTART was; civil reports that
// scale. candidateExists, when set, reports whether a candidate's wall clock is
// a local time the DTSTART's zone has (RFC 5545 §3.3.10), and gaps, when the
// resolver can list them, where that zone skips local times. Both are set only
// for a
// DATE-TIME carrying a TZID: a UTC value and a DATE name no local time, and a
// floating value is defined in no zone at all -- the zone RFC 4791 §7.3 reads
// it in for one request does not make the object's instances depend on that
// zone's transitions, so each floating instance is kept and placed per §3.3.5.
func recurrenceGenerationStart(component *Component, fallback time.Time, resolver RecurrenceTimeResolver) (start time.Time, resolveCandidate func(time.Time) (time.Time, bool), candidateExists func(time.Time) bool, gaps localTimeGaps, civil bool) {
	property, ok := ComponentProperty(component, "DTSTART")
	if !ok || hasZoneSuffix(strings.TrimSpace(property.Value)) {
		return fallback, func(candidate time.Time) (time.Time, bool) { return candidate, true }, nil, nil, false
	}
	wall, err := ParseDateTime(strings.TrimSpace(property.Value))
	if err != nil {
		return fallback, func(candidate time.Time) (time.Time, bool) { return candidate, true }, nil, nil, false
	}
	resolve := propertyTimeResolver(resolver)
	dateOnly := !strings.Contains(property.Value, "T")
	layout := "20060102T150405"
	if dateOnly {
		layout = "20060102"
	}
	resolveCandidate = func(candidate time.Time) (time.Time, bool) {
		return resolve.or(property.KeyPart, candidate.Format(layout))
	}
	if tzid, zoned := PropertyParam(property.KeyPart, "TZID"); zoned && !dateOnly {
		exists := localTimeExists(resolver)
		candidateExists = func(candidate time.Time) bool {
			return exists(tzid, candidate)
		}
		gaps = localTimeGapsOf(resolver, tzid)
	}
	return wall, resolveCandidate, candidateExists, gaps, true
}

// gapListingWindow is how far past a candidate the zone's gaps are listed at
// once. Candidates arrive in ascending order, so one listing serves a year of
// them.
const gapListingWindow = 366 * 24 * time.Hour

// existenceFromGaps answers exists from the zone's gaps, listed a window at a
// time: a wall clock outside every gap the zone lists exists, per the
// LocalTimeGapLister contract, so only one inside a gap is asked of exists.
// The first candidate is asked directly, since an expansion that stops at its
// first instance -- as a RFC 4791 §9.9 match does -- needs no listing. Where
// the gaps cannot be listed, every candidate is asked of exists, whose walks of
// an object's definition draw on the object's one ObservanceWork.
func existenceFromGaps(exists func(time.Time) bool, gaps localTimeGaps) func(time.Time) bool {
	var windowStart, windowEnd time.Time
	var listed []LocalTimeGap
	asked, listing, unlistable := false, false, false
	return func(wall time.Time) bool {
		if unlistable {
			return exists(wall)
		}
		if !asked {
			asked = true
			return exists(wall)
		}
		if !listing || wall.Before(windowStart) || wall.After(windowEnd) {
			through := wall.Add(gapListingWindow)
			found, ok := gaps(wall.Add(-maxLocalTimeGap), through)
			if !ok {
				unlistable = true
				return exists(wall)
			}
			windowStart, windowEnd, listed, listing = wall, through, found, true
		}
		for _, gap := range listed {
			if !wall.Before(gap.Start) && wall.Before(gap.Start.Add(gap.Length)) {
				return exists(wall)
			}
		}
		return true
	}
}

// fastForwardCountedSubDailyRecurrence jumps a fixed-step sub-daily rule from
// periodStart over the whole periods before threshold, and returns the period
// it reaches with the number of instances passed on the way. Once that number
// reaches remaining, the instances the rule still had to give, the jump may
// stop short of threshold: the series ends inside it.
//
// Each period of such a rule offers one candidate, which is an instance unless
// it falls on a local time the series' zone skips (RFC 5545 §3.3.10). Where the
// rule can meet such a time, gaps lists the zone's skipped local times, and
// each candidate inside one is tested as the period-by-period walk tests it;
// every candidate outside them exists. ok is false when the rule cannot be
// jumped this way, and the caller walks its periods.
func fastForwardCountedSubDailyRecurrence(periodStart, threshold, dtstart time.Time, rule recurrenceRule, remaining int, gaps localTimeGaps) (time.Time, int, bool) {
	stepSeconds, ok := subDailyRecurrenceSeconds(rule)
	if !ok || !fixedStepSubDailyRecurrence(rule) || (rule.localTimeExists != nil && gaps == nil) {
		return periodStart, 0, false
	}
	passed := 0
	for passed < remaining {
		steps := wholePeriodsBefore(periodStart, threshold, stepSeconds)
		if steps <= 0 {
			break
		}
		if rule.localTimeExists == nil {
			return addSeconds(periodStart, steps*stepSeconds), passed + int(steps), true
		}
		// The jump is held to the periods the series can still need, and
		// those a gap can take an instance from, so the gaps listed span
		// the series' end rather than everything up to threshold.
		steps = min(steps, int64(remaining-passed)+periodsWithinMaxGap(stepSeconds))
		skipped, ok := skippedSubDailyCandidates(periodStart, steps, stepSeconds, dtstart, rule, gaps)
		if !ok {
			break
		}
		periodStart = addSeconds(periodStart, steps*stepSeconds)
		passed += int(steps) - skipped
	}
	return periodStart, passed, true
}

// wholePeriodsBefore counts the periods of stepSeconds from periodStart that
// end at or before threshold. It counts whole seconds rather than a
// time.Duration, which saturates at about 292 years and would stop short for an
// early DTSTART.
func wholePeriodsBefore(periodStart, threshold time.Time, stepSeconds int64) int64 {
	if !threshold.After(periodStart) {
		return 0
	}
	elapsed := threshold.Unix() - periodStart.Unix()
	if threshold.Nanosecond() < periodStart.Nanosecond() {
		elapsed--
	}
	return elapsed / stepSeconds
}

// maxLocalTimeGap bounds the length of a stretch of local time a zone skips:
// a UTC offset is less than a day either side of UTC.
const maxLocalTimeGap = 48 * time.Hour

// periodsWithinMaxGap is how many periods of stepSeconds one gap can hold.
func periodsWithinMaxGap(stepSeconds int64) int64 {
	return int64(maxLocalTimeGap/time.Second)/stepSeconds + 1
}

// skippedSubDailyCandidates counts the candidates of the steps periods from
// periodStart of a fixed-step sub-daily rule that existingLocalTimes leaves
// out. The candidates are steps apart from the first period's, so only those
// inside a gap are tested.
func skippedSubDailyCandidates(periodStart time.Time, steps, stepSeconds int64, dtstart time.Time, rule recurrenceRule, gaps localTimeGaps) (int, bool) {
	firsts := subDailyCandidatesForPeriod(periodStart, dtstart, rule)
	if len(firsts) != 1 {
		return 0, false
	}
	first := firsts[0]
	last := addSeconds(first, (steps-1)*stepSeconds)
	found, ok := gaps(first.Add(-maxLocalTimeGap), last)
	if !ok {
		return 0, false
	}
	slices.SortFunc(found, func(a, b LocalTimeGap) int { return a.Start.Compare(b.Start) })
	skipped := 0
	// tested is the first candidate index no gap has covered yet, so a
	// candidate inside gaps that overlap is tested once.
	var tested int64
	for _, gap := range found {
		from := max(tested, ceilDivide(gap.Start.Unix()-first.Unix(), stepSeconds))
		through := min(steps, ceilDivide(gap.Start.Add(gap.Length).Unix()-first.Unix(), stepSeconds))
		for index := from; index < through; index++ {
			candidate := addSeconds(first, index*stepSeconds)
			if !candidate.Equal(dtstart) && !rule.localTimeExists(candidate) {
				skipped++
			}
		}
		tested = max(tested, through)
	}
	return skipped, true
}

// ceilDivide is numerator/denominator rounded toward positive infinity, for a
// positive denominator.
func ceilDivide(numerator, denominator int64) int64 {
	quotient := numerator / denominator
	if numerator%denominator > 0 {
		quotient++
	}
	return quotient
}

func fixedStepSubDailyRecurrence(rule recurrenceRule) bool {
	if rule.Freq != "SECONDLY" && rule.Freq != "MINUTELY" && rule.Freq != "HOURLY" {
		return false
	}
	return len(rule.BySecond) == 0 &&
		len(rule.ByMinute) == 0 &&
		len(rule.ByHour) == 0 &&
		len(rule.ByMonth) == 0 &&
		len(rule.ByMonthDay) == 0 &&
		len(rule.ByYearDay) == 0 &&
		len(rule.ByWeekNo) == 0 &&
		len(rule.ByDay) == 0 &&
		len(rule.BySetPos) == 0
}

func supportedRecurrenceFreq(freq string) bool {
	switch strings.ToUpper(freq) {
	case "SECONDLY", "MINUTELY", "HOURLY", "DAILY", "WEEKLY", "MONTHLY", "YEARLY":
		return true
	default:
		return false
	}
}

func periodOverlaps(start, end, rangeStart, rangeEnd time.Time) bool {
	return start.Before(rangeEnd) && end.After(rangeStart)
}

// periodTouches is periodOverlaps with both bounds inclusive, so an occurrence
// that only meets the range at an endpoint still reaches it.
func periodTouches(start, end, rangeStart, rangeEnd time.Time) bool {
	return !start.After(rangeEnd) && !end.Before(rangeStart)
}

// recurrenceInstant is a comparable form of an absolute instant, so a set of
// them can be looked up rather than scanned. Seconds and nanoseconds are held
// apart rather than combined: RFC 5545 §3.3.5 admits a DATE-TIME centuries
// before 1678, which a nanosecond count cannot represent.
type recurrenceInstant struct {
	seconds     int64
	nanoseconds int
}

// occurrenceKey identifies an instance of a recurrence set: by the instant its
// slot names, or, onDate, by the date it names, held as its UTC midnight.
type occurrenceKey struct {
	instant recurrenceInstant
	onDate  bool
}

// recurrenceInstantSet answers the exclusion test the expansion applies once
// per generated instance. RFC 5545 §3.8.5.1 puts no bound on how many EXDATEs a
// component may carry, so scanning them would make one expansion cost the
// product of the two counts.
type recurrenceInstantSet map[recurrenceInstant]struct{}

func (s recurrenceInstantSet) add(value time.Time) {
	s[recurrenceInstant{seconds: value.Unix(), nanoseconds: value.Nanosecond()}] = struct{}{}
}

func (s recurrenceInstantSet) contains(value time.Time) bool {
	_, ok := s[recurrenceInstant{seconds: value.Unix(), nanoseconds: value.Nanosecond()}]
	return ok
}

func componentRDatePeriods(component *Component, resolve PropertyTimeResolver) []rdatePeriod {
	var periods []rdatePeriod
	for _, prop := range componentProperties(component, "RDATE") {
		for _, value := range strings.Split(prop.Value, ",") {
			value = strings.TrimSpace(value)
			if value == "" {
				continue
			}
			if strings.Contains(value, "/") {
				parts := strings.SplitN(value, "/", 2)
				start, ok := resolve.or(prop.KeyPart, strings.TrimSpace(parts[0]))
				if !ok {
					continue
				}
				var end time.Time
				periodEnd := strings.TrimSpace(parts[1])
				if strings.HasPrefix(strings.ToUpper(periodEnd), "P") {
					if duration, ok := ParseDuration(periodEnd); ok {
						end = start.Add(duration)
					}
				} else {
					if parsedEnd, ok := resolve.or(prop.KeyPart, periodEnd); ok {
						end = parsedEnd
					}
				}
				date, _ := DateOf(parts[0])
				periods = append(periods, rdatePeriod{Start: start, End: end, Date: date})
				continue
			}
			if start, ok := resolve.or(prop.KeyPart, value); ok {
				date, _ := DateOf(value)
				onDate := len(value) == len(DateLayout) || PropertyParamEquals(prop.KeyPart, "VALUE", "DATE")
				periods = append(periods, rdatePeriod{Start: start, Date: date, onDate: onDate})
			}
		}
	}
	return periods
}

// exclusions are the slots a series' EXDATEs leave out: by date for a DATE in
// a series starting on a DATE, whose slots are identified by their dates, and
// by instant otherwise.
type exclusions struct {
	instants recurrenceInstantSet
	dates    map[int64]struct{}
}

// excludes reports whether the slot at instant, on date when the series
// starts on a DATE, is left out.
func (x exclusions) excludes(instant, date time.Time) bool {
	if _, ok := x.dates[date.Unix()]; ok && !date.IsZero() {
		return true
	}
	return x.instants.contains(instant)
}

func eventExDates(component *Component, resolve PropertyTimeResolver, byDate bool) exclusions {
	excluded := exclusions{instants: make(recurrenceInstantSet)}
	for _, prop := range componentProperties(component, "EXDATE") {
		propOnDate := PropertyParamEquals(prop.KeyPart, "VALUE", "DATE")
		for _, value := range strings.Split(prop.Value, ",") {
			value = strings.TrimSpace(value)
			if byDate && (propOnDate || len(value) == len(DateLayout)) {
				if date, ok := DateOf(value); ok {
					if excluded.dates == nil {
						excluded.dates = make(map[int64]struct{})
					}
					excluded.dates[date.Unix()] = struct{}{}
				}
				continue
			}
			if parsed, ok := resolve.or(prop.KeyPart, value); ok {
				excluded.instants.add(parsed)
			}
		}
	}
	return excluded
}

type recurrenceOverride struct {
	recurrenceID       time.Time
	period             BusyPeriod
	cancelled          bool
	rangeThisAndFuture bool
	// recurrenceIDDate is the date its RECURRENCE-ID names, as DateOf reads
	// it, when the override is placed or identified by dates: it names a DATE
	// slot of a series starting on a DATE, or starts on a DATE itself.
	recurrenceIDDate time.Time
	// namesDate reports a RECURRENCE-ID naming its slot by that date, as
	// RecurrenceIDNamesDate has it; any other names the slot at its instant.
	namesDate bool
	// startDate is the date an override starting on a DATE starts on, days
	// the days it spans, and startKeyPart the DTSTART key part its dates
	// resolve through; startDate is zero for one starting at a DATE-TIME.
	startDate    time.Time
	days         int
	startKeyPart string
}

// onDate reports an override starting on a DATE.
func (o recurrenceOverride) onDate() bool {
	return !o.startDate.IsZero()
}

// componentRecurrenceOverrides reads each override of a componentName series
// by the EventEnd rule, its end the one it writes: a VTODO's DUE, any other
// component's DTEND, or its DURATION. A VEVENT override writing neither takes
// seriesLength, the length of an occurrence of its series, when
// EventEnd.InheritSeriesLength gives it that length -- when its DTSTART is a
// DATE exactly when its series' is, as seriesStartIsDate reports -- and
// otherwise lasts as its DTSTART alone has it last. Any other override writing
// neither ends as RFC 4791 §9.9 places a component with a DTSTART alone, at
// that start. One starting on a DATE spans its Days from that date, each
// midnight placed through midnights.
func componentRecurrenceOverrides(components []Component, resolve PropertyTimeResolver, midnights *dateMidnights, componentName string, seriesStartIsDate bool, seriesLength time.Duration) []recurrenceOverride {
	endName := "DTEND"
	if strings.EqualFold(componentName, "VTODO") {
		endName = "DUE"
	}
	var overrides []recurrenceOverride
	for _, component := range components {
		recurrenceIDProp, ok := ComponentProperty(&component, "RECURRENCE-ID")
		if !ok {
			continue
		}
		recurrenceID, ok := resolve.or(recurrenceIDProp.KeyPart, recurrenceIDProp.Value)
		if !ok {
			continue
		}

		start, startProp := recurrenceID, recurrenceIDProp
		if prop, ok := ComponentProperty(&component, "DTSTART"); ok {
			if parsed, ok := resolve.or(prop.KeyPart, prop.Value); ok {
				start, startProp = parsed, prop
			}
		}

		cancelled := false
		if prop, ok := ComponentProperty(&component, "STATUS"); ok {
			cancelled = strings.EqualFold(strings.TrimSpace(prop.Value), "CANCELLED")
		}

		extent := EventEnd{Start: start, StartIsDate: dateOnlyValue(startProp)}
		if extent.StartIsDate {
			extent.StartDate, _ = DateOf(startProp.Value)
		}
		endProp, writesEnd := ComponentProperty(&component, endName)
		if writesEnd {
			extent.End, extent.HasEnd = resolve.or(endProp.KeyPart, endProp.Value)
			if extent.StartIsDate && dateOnlyValue(endProp) {
				extent.EndDate, _ = DateOf(endProp.Value)
			}
		}
		duration, writesDuration := ComponentProperty(&component, "DURATION")
		if writesDuration {
			extent.Duration, extent.HasDuration = ParseDuration(duration.Value)
		}
		if strings.EqualFold(componentName, "VEVENT") && !writesEnd && !writesDuration {
			extent.InheritSeriesLength(seriesStartIsDate, seriesLength)
		}

		override := recurrenceOverride{
			recurrenceID:       recurrenceID,
			cancelled:          cancelled,
			rangeThisAndFuture: PropertyParamEquals(recurrenceIDProp.KeyPart, "RANGE", "THISANDFUTURE"),
		}
		if extent.StartIsDate && !extent.StartDate.IsZero() {
			override.startDate, override.days, override.startKeyPart = extent.StartDate, extent.Days(), startProp.KeyPart
		}
		override.namesDate = seriesStartIsDate && dateOnlyValue(recurrenceIDProp)
		if override.namesDate || override.onDate() {
			override.recurrenceIDDate, _ = DateOf(recurrenceIDProp.Value)
		}
		override.period = BusyPeriod{Start: start, End: extent.OccurrenceEnd(func(date time.Time) time.Time {
			return midnights.at(startProp.KeyPart, date)
		})}
		overrides = append(overrides, override)
	}
	return overrides
}

// dateOnlyValue reports whether a date-valued property holds a DATE rather than
// a DATE-TIME.
func dateOnlyValue(property PropertyValue) bool {
	return len(strings.TrimSpace(property.Value)) == len(DateLayout) || PropertyParamEquals(property.KeyPart, "VALUE", "DATE")
}

// seriesLength is how long each instance of a series lasts. One starting at a
// DATE-TIME lasts duration. One starting on a DATE is identified by its date
// and spans whole days, duration's NominalDays of them unless a PERIOD RDATE
// on the same date gives it its own, from that date's midnight to the one
// that many dates on, each placed in the series' zone through the DTSTART's
// key part, however many hours those days hold.
type seriesLength struct {
	duration   time.Duration
	onDate     bool
	days       int
	keyPart    string
	midnights  *dateMidnights
	periodDays map[int64]int
}

func newSeriesLength(component *Component, duration time.Duration, midnights *dateMidnights) seriesLength {
	length := seriesLength{duration: duration, midnights: midnights}
	if prop, ok := ComponentProperty(component, "DTSTART"); ok && dateOnlyValue(prop) {
		length.onDate, length.days, length.keyPart = true, NominalDays(duration), prop.KeyPart
	}
	return length
}

// setPeriodDays gives the instance on date the days a PERIOD RDATE naming it
// spans.
func (l *seriesLength) setPeriodDays(date time.Time, days int) {
	if l.periodDays == nil {
		l.periodDays = make(map[int64]int)
	}
	l.periodDays[date.Unix()] = days
}

// span is the period an instance on date spanning days days occupies.
func (l seriesLength) span(date time.Time, days int) BusyPeriod {
	return BusyPeriod{Start: l.midnights.at(l.keyPart, date), End: l.midnights.at(l.keyPart, date.AddDate(0, 0, days))}
}

// instance is the period the instance starting at start, on date for a
// series starting on a DATE, occupies, and the dates it falls on.
func (l seriesLength) instance(start, date time.Time) (BusyPeriod, occurrenceDays) {
	if !l.onDate || date.IsZero() {
		return BusyPeriod{Start: start, End: start.Add(l.duration)}, occurrenceDays{slot: date}
	}
	days := l.days
	if periodDays, ok := l.periodDays[date.Unix()]; ok {
		days = periodDays
	}
	return l.span(date, days), occurrenceDays{slot: date, start: date, days: days}
}

// recurrenceOverrideIndex answers the two override questions the expansion
// asks of every generated instance: whether an ordinary override replaces its
// slot, and which RANGE=THISANDFUTURE override governs it. RFC 5545 §3.8.4.4
// bounds neither how many overrides a resource may carry, so both are lookups
// rather than scans; otherwise one expansion would cost the product of the
// instance and override counts. In a series starting on a DATE the overrides
// naming DATE slots are matched to them by date.
type recurrenceOverrideIndex struct {
	exact      recurrenceInstantSet
	exactDates map[int64]struct{}
	// thisAndFuture is ordered by RECURRENCE-ID, or by its date when byDate.
	// Among overrides naming the same slot the first in the resource comes
	// first, and is the one that governs.
	thisAndFuture []recurrenceOverride
	byDate        bool
	// thisAndFutureOnDate reports a RANGE=THISANDFUTURE override starting on
	// a DATE, whose instances move by dates, and midnights places them.
	thisAndFutureOnDate bool
	midnights           *dateMidnights
}

func newRecurrenceOverrideIndex(overrides []recurrenceOverride, seriesOnDate bool, midnights *dateMidnights) recurrenceOverrideIndex {
	index := recurrenceOverrideIndex{exact: make(recurrenceInstantSet), midnights: midnights, byDate: seriesOnDate}
	for _, override := range overrides {
		if override.rangeThisAndFuture {
			index.thisAndFuture = append(index.thisAndFuture, override)
			index.thisAndFutureOnDate = index.thisAndFutureOnDate || override.onDate()
			index.byDate = index.byDate && override.namesDate
			continue
		}
		if override.namesDate {
			if index.exactDates == nil {
				index.exactDates = make(map[int64]struct{})
			}
			index.exactDates[override.recurrenceIDDate.Unix()] = struct{}{}
			continue
		}
		index.exact.add(override.recurrenceID)
	}
	sort.SliceStable(index.thisAndFuture, func(i, j int) bool {
		return index.slotOf(index.thisAndFuture[i]).Before(index.slotOf(index.thisAndFuture[j]))
	})
	return index
}

// slotOf is what override's RECURRENCE-ID is ordered and matched by.
func (index recurrenceOverrideIndex) slotOf(override recurrenceOverride) time.Time {
	if index.byDate {
		return override.recurrenceIDDate
	}
	return override.recurrenceID
}

// replaces reports whether an ordinary override names the slot at
// recurrenceID, on date when its series starts on a DATE.
func (index recurrenceOverrideIndex) replaces(recurrenceID, date time.Time) bool {
	if _, ok := index.exactDates[date.Unix()]; ok && !date.IsZero() {
		return true
	}
	return index.exact.contains(recurrenceID)
}

// governing returns the RANGE=THISANDFUTURE override with the latest
// RECURRENCE-ID at or before the slot at start, on date when its series
// starts on a DATE.
func (index recurrenceOverrideIndex) governing(start, date time.Time) (recurrenceOverride, bool) {
	if index.byDate {
		start = date
	}
	overrides := index.thisAndFuture
	after := sort.Search(len(overrides), func(i int) bool {
		return index.slotOf(overrides[i]).After(start)
	})
	if after == 0 {
		return recurrenceOverride{}, false
	}
	latest := index.slotOf(overrides[after-1])
	first := sort.Search(after, func(i int) bool {
		return !index.slotOf(overrides[i]).Before(latest)
	})
	return overrides[first], true
}

func (index recurrenceOverrideIndex) hasThisAndFuture() bool {
	return len(index.thisAndFuture) > 0
}

// maxThisAndFutureLengthSeconds is the longest occurrence any
// RANGE=THISANDFUTURE override gives the instances it governs, in whole
// seconds rounded up.
func (index recurrenceOverrideIndex) maxThisAndFutureLengthSeconds() int64 {
	var longest int64
	for _, override := range index.thisAndFuture {
		if override.cancelled {
			continue
		}
		if length := override.period.End.Unix() - override.period.Start.Unix() + 1; length > longest {
			longest = length
		}
	}
	return longest
}

// maxThisAndFutureShiftSeconds is the furthest any RANGE=THISANDFUTURE
// override moves its instances, in whole seconds rounded up. It is counted in
// seconds rather than as a time.Duration because the distance can exceed the
// ~292 years a Duration holds.
func (index recurrenceOverrideIndex) maxThisAndFutureShiftSeconds() int64 {
	var max int64
	for _, override := range index.thisAndFuture {
		if override.cancelled {
			continue
		}
		shift := override.period.Start.Unix() - override.recurrenceID.Unix()
		if shift < 0 {
			shift = -shift
		}
		if shift+1 > max {
			max = shift + 1
		}
	}
	return max
}

// addSeconds moves t by seconds, which may exceed what a time.Duration holds.
func addSeconds(t time.Time, seconds int64) time.Time {
	return time.Unix(t.Unix()+seconds, int64(t.Nanosecond())).In(t.Location())
}

// moveInstant moves t by the distance from from to to, which may exceed what
// a time.Duration holds.
func moveInstant(t, from, to time.Time) time.Time {
	return time.Unix(t.Unix()+(to.Unix()-from.Unix()),
		int64(t.Nanosecond())+int64(to.Nanosecond()-from.Nanosecond())).In(t.Location())
}

// applyThisAndFuture is where the RANGE=THISANDFUTURE override governing the
// slot that period fills, on the dates dates holds, places it: moved as far
// from the slot as the override is from the slot it names, and lasting as
// long as the override. An override starting on a DATE moves it by the dates
// between its RECURRENCE-ID and its DTSTART as written, and it spans the
// override's days from the date it lands on; the hours between the two would
// move it a day too far past a date its zone skips. It reports true for a
// slot a cancelled override removes.
func (index recurrenceOverrideIndex) applyThisAndFuture(period BusyPeriod, dates occurrenceDays) (BusyPeriod, occurrenceDays, bool) {
	selected, ok := index.governing(period.Start, dates.slot)
	if !ok {
		return period, dates, false
	}
	if selected.cancelled {
		return period, dates, true
	}

	if selected.onDate() && !dates.slot.IsZero() && !selected.recurrenceIDDate.IsZero() {
		date := dates.slot.AddDate(0, 0, CivilDaysBetween(selected.recurrenceIDDate, selected.startDate))
		moved := BusyPeriod{
			Start: index.midnights.at(selected.startKeyPart, date),
			End:   index.midnights.at(selected.startKeyPart, date.AddDate(0, 0, selected.days)),
		}
		return moved, occurrenceDays{slot: dates.slot, start: date, days: selected.days}, false
	}
	shiftedStart := moveInstant(period.Start, selected.recurrenceID, selected.period.Start)
	duration := selected.period.End.Sub(selected.period.Start)
	return BusyPeriod{Start: shiftedStart, End: shiftedStart.Add(duration)}, occurrenceDays{slot: dates.slot}, false
}

// Component is one component block's parsed content lines, of whatever type the
// BEGIN named.
type Component struct {
	properties          []PropertyValue
	malformedProperties []string
}

// PropertyValue is one content line of a component: the part before the
// first colon (name plus parameters) and the value after it.
type PropertyValue struct {
	KeyPart string
	Value   string
}

// PrimaryVEventComponent returns the master VEVENT of the resource, the
// component the denormalized event columns are derived from.
func PrimaryVEventComponent(ical string) *Component {
	return PrimaryComponent(ical, "VEVENT")
}

// PrimaryComponent returns the master component of the named type: the first
// one carrying no RECURRENCE-ID, since RFC 4791 §4.1 permits a resource made
// only of overridden instances and every one of those carries the property.
func PrimaryComponent(ical, componentName string) *Component {
	return primaryOf(componentsNamed(ical, componentName))
}

func primaryOf(components []Component) *Component {
	for i := range components {
		if !componentHasProperty(&components[i], "RECURRENCE-ID") {
			return &components[i]
		}
	}
	if len(components) == 0 {
		return nil
	}
	return &components[0]
}

func componentsNamed(ical, componentName string) []Component {
	return componentsNamedFromLines(UnfoldLines(ical), componentName)
}

func componentsNamedFromLines(lines []string, componentName string) []Component {
	return topLevelComponentsFromLines(lines, func(name string) bool {
		return strings.EqualFold(name, componentName)
	})
}

func topLevelComponents(ical string, accept func(string) bool) []Component {
	return topLevelComponentsFromLines(UnfoldLines(ical), accept)
}

// topLevelComponentsFromLines reads the components directly beneath a
// VCALENDAR that accept selects, in the order they begin, as ReadComponents
// reads a stored object, so the DAV layer and this package see the same
// components. A property belongs to the innermost component open around it, so
// one that follows a sub-component's END is the enclosing component's.
func topLevelComponentsFromLines(lines []string, accept func(string) bool) []Component {
	// A handle is the index of the component it collects, or -1 for the root.
	var components []Component
	root := -1
	_, _ = ReadComponents(lines, false,
		func(parent *int, name string) (*int, error) {
			switch {
			case parent == nil:
				return &root, nil
			case parent != &root || !accept(name):
				return nil, nil
			}
			index := len(components)
			components = append(components, Component{})
			return &index, nil
		},
		func(component *int, line string) error {
			if *component < 0 {
				return nil
			}
			current := &components[*component]
			keyPart, value, ok := SplitContentLine(line)
			if !ok {
				current.malformedProperties = append(current.malformedProperties, line)
				return nil
			}
			current.properties = append(current.properties, PropertyValue{KeyPart: keyPart, Value: value})
			return nil
		})
	return components
}

func componentProperties(component *Component, name string) []PropertyValue {
	var values []PropertyValue
	if component == nil {
		return values
	}
	for _, prop := range component.properties {
		if !strings.EqualFold(PropertyName(prop.KeyPart), name) {
			continue
		}
		values = append(values, prop)
	}
	return values
}

func ComponentProperty(component *Component, name string) (PropertyValue, bool) {
	if component == nil {
		return PropertyValue{}, false
	}
	for _, prop := range component.properties {
		if strings.EqualFold(PropertyName(prop.KeyPart), name) {
			return prop, true
		}
	}
	return PropertyValue{}, false
}

func componentPropertyValue(component *Component, name string) string {
	prop, ok := ComponentProperty(component, name)
	if !ok {
		return ""
	}
	return strings.TrimSpace(prop.Value)
}

func componentHasProperty(component *Component, name string) bool {
	_, ok := ComponentProperty(component, name)
	return ok
}

// PropertyParam returns the value of the named parameter on a content line's
// key part, and whether the line carries it at all.
//
// RFC 5545 §3.2 makes a parameter value a quoted string whenever it contains a
// colon, a semicolon or a comma, and some clients quote unconditionally --
// Exchange writes every TZID that way. The quotes delimit the value rather than
// belonging to it, so a separator inside them does not start another parameter
// and they are stripped from what is returned: a caller handed
// `"America/New_York"` resolves no zone at all and silently places every value
// it reads a whole UTC offset away from where the property named.
func PropertyParam(keyPart, param string) (string, bool) {
	for _, part := range propertyParameterParts(keyPart) {
		name, value, found := strings.Cut(part, "=")
		if !found {
			continue
		}
		if strings.EqualFold(strings.TrimSpace(name), param) {
			return unquotePropertyParam(strings.TrimSpace(value)), true
		}
	}
	return "", false
}

// SplitContentLine splits an unfolded content line into its key part (name and
// parameters) and its value. RFC 5545 §3.1 lets a quoted parameter value hold a
// colon, so the value starts at the first colon outside double quotes: Exchange
// writes zone names such as "(UTC-05:00) Eastern Time (US & Canada)" as TZIDs.
func SplitContentLine(line string) (keyPart, value string, ok bool) {
	colon := IndexOutsideQuotes(line, ':')
	if colon < 0 {
		return "", "", false
	}
	return line[:colon], line[colon+1:], true
}

// IndexOutsideQuotes returns the index of the first delimiter in a content line
// or key part that is not inside a double-quoted parameter value, or -1.
func IndexOutsideQuotes(s string, delimiter byte) int {
	quoted := false
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '"':
			quoted = !quoted
		case delimiter:
			if !quoted {
				return i
			}
		}
	}
	return -1
}

// propertyParameterParts splits a content line's key part into its parameters,
// leaving out the property name that precedes the first separator.
func propertyParameterParts(keyPart string) []string {
	separator := IndexOutsideQuotes(keyPart, ';')
	if separator < 0 {
		return nil
	}
	var parts []string
	rest := keyPart[separator+1:]
	for {
		next := IndexOutsideQuotes(rest, ';')
		if next < 0 {
			return append(parts, rest)
		}
		parts = append(parts, rest[:next])
		rest = rest[next+1:]
	}
}

func unquotePropertyParam(value string) string {
	if len(value) >= 2 && value[0] == '"' && value[len(value)-1] == '"' {
		return value[1 : len(value)-1]
	}
	return value
}

func PropertyParamEquals(keyPart, param, value string) bool {
	got, ok := PropertyParam(keyPart, param)
	return ok && strings.EqualFold(got, value)
}

// PropertyName returns the property name from a content line or its key part,
// i.e. everything before the first parameter (";") or value (":") delimiter.
// A key part carrying neither delimiter is returned unchanged.
func PropertyName(keyPart string) string {
	if idx := strings.IndexAny(keyPart, ";:"); idx >= 0 {
		return keyPart[:idx]
	}
	return keyPart
}

// ParsePropertyDateTimeLocal resolves one date-valued content line to an
// absolute instant, reading a TZID from the property's parameters when it names
// a zone the host knows. A value carrying neither a resolvable TZID nor its own
// zone suffix is floating, and this reads it as UTC -- the fallback RFC 4791
// §7.3 reaches when a request carries no CALDAV:timezone and the collection
// defines no CALDAV:calendar-timezone. A caller holding either of those resolves
// the value against it instead.
func ParsePropertyDateTimeLocal(keyPart, value string) (time.Time, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return time.Time{}, false
	}
	if tzid, ok := PropertyParam(keyPart, "TZID"); ok {
		if loc, ok := HostLocation(tzid); ok {
			if parsed, err := ParseDateTimeInLocation(value, loc); err == nil {
				return parsed, true
			}
		}
	}
	parsed, err := ParseDateTime(value)
	return parsed, err == nil
}

func ParseDuration(value string) (time.Duration, bool) {
	value = strings.TrimSpace(strings.ToUpper(value))
	if value == "" {
		return 0, false
	}
	sign := time.Duration(1)
	if strings.HasPrefix(value, "-") {
		sign = -1
		value = strings.TrimPrefix(value, "-")
	} else {
		value = strings.TrimPrefix(value, "+")
	}
	if !strings.HasPrefix(value, "P") {
		return 0, false
	}
	value = strings.TrimPrefix(value, "P")
	if value == "" {
		return 0, false
	}

	var total time.Duration
	var number strings.Builder
	inTime := false
	consume := func(unit byte) bool {
		if number.Len() == 0 {
			return false
		}
		n, err := strconv.Atoi(number.String())
		number.Reset()
		if err != nil {
			return false
		}
		switch unit {
		case 'W':
			total += time.Duration(n) * 7 * 24 * time.Hour
		case 'D':
			total += time.Duration(n) * 24 * time.Hour
		case 'H':
			total += time.Duration(n) * time.Hour
		case 'M':
			if !inTime {
				return false
			}
			total += time.Duration(n) * time.Minute
		case 'S':
			total += time.Duration(n) * time.Second
		default:
			return false
		}
		return true
	}

	for i := 0; i < len(value); i++ {
		ch := value[i]
		if ch >= '0' && ch <= '9' {
			number.WriteByte(ch)
			continue
		}
		if ch == 'T' {
			if inTime || number.Len() != 0 {
				return 0, false
			}
			inTime = true
			continue
		}
		if !consume(ch) {
			return 0, false
		}
	}
	if number.Len() != 0 || total < 0 {
		return 0, false
	}
	return sign * total, true
}

const (
	recurrenceScanLimit = 100000
	maxICalendarInteger = 1<<31 - 1

	// unboundedRecurrenceHorizonYears is the window over which the instance
	// limit is measured for a rule with neither COUNT nor UNTIL. A year is
	// long enough that every calendar frequency a client sends stays well
	// inside the limit, and short enough that a sub-daily rule is past it.
	unboundedRecurrenceHorizonYears = 1
)

// recurrenceRule is a parsed RRULE. Its integer BY lists are sorted and free of
// duplicates, and its BYDAY list is free of duplicates, which is what bounds the
// work a rule can ask of each day it is matched against however its text
// repeats them.
type recurrenceRule struct {
	Freq       string
	Interval   int
	Count      int
	Until      *time.Time
	WKST       time.Weekday
	BySecond   []int
	ByMinute   []int
	ByHour     []int
	ByMonth    []int
	ByMonthDay []int
	ByYearDay  []int
	ByWeekNo   []int
	ByDay      []weekdaySpecifier
	BySetPos   []int

	// byDaySet is ByDay in the form a day is matched against.
	// parseRecurrenceRule builds it, as anything else assembling a rule must.
	byDaySet weekdaySet
	// dayMask holds the rule's recurrenceDayMask once days builds it. It is
	// shared by every copy of the parsed rule, none of which changes the date
	// filters the mask is built from.
	dayMask *lazyRecurrenceDayMask

	// localTimeExists, when set, reports whether a candidate's wall clock is a
	// local time the series' zone has. RFC 5545 §3.3.10 requires a candidate
	// on a time a transition skips to be "ignored" and not "counted as part of
	// the recurrence set", so visitRecurrenceCandidates never offers one. It is
	// left nil for a VTIMEZONE observance rule, whose onsets are transitions
	// stated in the offset they leave rather than instances of a series.
	localTimeExists func(wall time.Time) bool
}

type weekdaySpecifier struct {
	Ordinal int
	Day     time.Weekday
}

// parseRecurrenceRule parses rrule. loc and resolve read a floating UNTIL, per
// parseRecurrenceUntil, and startWall reads the wall clock of a DATE-TIME
// DTSTART; it is nil for a DATE DTSTART.
func parseRecurrenceRule(rrule string, loc *time.Location, resolve PropertyTimeResolver, startWall wallClockResolver) (recurrenceRule, bool) {
	params := make(map[string]string)
	for _, part := range strings.Split(rrule, ";") {
		kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
		if len(kv) != 2 {
			return recurrenceRule{}, false
		}
		key := strings.ToUpper(strings.TrimSpace(kv[0]))
		value := strings.TrimSpace(kv[1])
		if !knownRecurrenceRulePart(key) || value == "" {
			return recurrenceRule{}, false
		}
		if _, duplicate := params[key]; duplicate {
			return recurrenceRule{}, false
		}
		params[key] = value
	}
	freq := strings.ToUpper(params["FREQ"])
	if !supportedRecurrenceFreq(freq) {
		return recurrenceRule{}, false
	}
	rule := recurrenceRule{
		Freq:     freq,
		Interval: 1,
		WKST:     time.Monday,
	}
	if intervalStr := params["INTERVAL"]; intervalStr != "" {
		interval, err := strconv.Atoi(intervalStr)
		if err != nil || interval <= 0 || interval > maxICalendarInteger {
			return recurrenceRule{}, false
		}
		rule.Interval = interval
	}
	if countStr := params["COUNT"]; countStr != "" {
		count, err := strconv.Atoi(countStr)
		if err != nil || count <= 0 || count > maxICalendarInteger {
			return recurrenceRule{}, false
		}
		rule.Count = count
	}
	if params["COUNT"] != "" && params["UNTIL"] != "" {
		return recurrenceRule{}, false
	}
	if untilStr := params["UNTIL"]; untilStr != "" {
		until, ok := parseRecurrenceUntil(untilStr, loc, resolve, startWall)
		if !ok {
			return recurrenceRule{}, false
		}
		rule.Until = &until
	}
	if wkst := params["WKST"]; wkst != "" {
		day, ok := parseWeekday(wkst)
		if !ok {
			return recurrenceRule{}, false
		}
		rule.WKST = day
	}

	var ok bool
	if rule.BySecond, ok = parseIntList(params["BYSECOND"], 0, 60); !ok {
		return recurrenceRule{}, false
	}
	if rule.ByMinute, ok = parseIntList(params["BYMINUTE"], 0, 59); !ok {
		return recurrenceRule{}, false
	}
	if rule.ByHour, ok = parseIntList(params["BYHOUR"], 0, 23); !ok {
		return recurrenceRule{}, false
	}
	if rule.ByMonth, ok = parseIntList(params["BYMONTH"], 1, 12); !ok {
		return recurrenceRule{}, false
	}
	if rule.ByMonthDay, ok = parseIntListAllowNegative(params["BYMONTHDAY"], -31, 31); !ok {
		return recurrenceRule{}, false
	}
	if rule.ByYearDay, ok = parseIntListAllowNegative(params["BYYEARDAY"], -366, 366); !ok {
		return recurrenceRule{}, false
	}
	if rule.ByWeekNo, ok = parseIntListAllowNegative(params["BYWEEKNO"], -53, 53); !ok {
		return recurrenceRule{}, false
	}
	if rule.BySetPos, ok = parseIntListAllowNegative(params["BYSETPOS"], -366, 366); !ok {
		return recurrenceRule{}, false
	}
	if rule.ByDay, ok = parseWeekdayList(params["BYDAY"]); !ok {
		return recurrenceRule{}, false
	}
	rule.byDaySet = newWeekdaySet(rule.ByDay)
	rule.dayMask = &lazyRecurrenceDayMask{}
	ordinalWeekday := false
	for _, day := range rule.ByDay {
		if day.Ordinal != 0 {
			ordinalWeekday = true
			break
		}
	}
	if ordinalWeekday && rule.Freq != "MONTHLY" && rule.Freq != "YEARLY" {
		return recurrenceRule{}, false
	}
	if ordinalWeekday && rule.Freq == "YEARLY" && len(rule.ByWeekNo) != 0 {
		return recurrenceRule{}, false
	}
	if rule.Freq == "WEEKLY" && len(rule.ByMonthDay) != 0 {
		return recurrenceRule{}, false
	}
	if (rule.Freq == "DAILY" || rule.Freq == "WEEKLY" || rule.Freq == "MONTHLY") && len(rule.ByYearDay) != 0 {
		return recurrenceRule{}, false
	}
	if len(rule.ByWeekNo) != 0 && rule.Freq != "YEARLY" {
		return recurrenceRule{}, false
	}
	if len(rule.BySetPos) != 0 && !recurrenceRuleHasOtherByPart(rule) {
		return recurrenceRule{}, false
	}
	return rule, true
}

func recurrenceRuleHasOtherByPart(rule recurrenceRule) bool {
	return len(rule.BySecond) != 0 || len(rule.ByMinute) != 0 || len(rule.ByHour) != 0 ||
		len(rule.ByDay) != 0 || len(rule.ByMonthDay) != 0 || len(rule.ByYearDay) != 0 ||
		len(rule.ByWeekNo) != 0 || len(rule.ByMonth) != 0
}

func knownRecurrenceRulePart(name string) bool {
	switch name {
	case "FREQ", "UNTIL", "COUNT", "INTERVAL", "BYSECOND", "BYMINUTE", "BYHOUR",
		"BYDAY", "BYMONTHDAY", "BYYEARDAY", "BYWEEKNO", "BYMONTH", "BYSETPOS", "WKST":
		return true
	default:
		return false
	}
}

// parseRecurrenceUntil reads the UNTIL rule part. RFC 5545 §3.3.10 requires a
// floating UNTIL exactly where DTSTART is floating, and a UTC one beside a
// DTSTART with a TZID, so a value carrying no zone of its own belongs to
// whatever zone resolved DTSTART. Beside a DATE-TIME DTSTART it is read on the
// DTSTART's wall clock, through startWall: in the zone the DTSTART's TZID
// names, which reading it as a floating value would lose, and for a floating
// DTSTART wherever that is read. Beside a DATE DTSTART it is read by the
// caller's resolver first, since that is the only thing holding a zone the
// observances of the resource itself define, then in loc.
//
// UNTIL is inclusive, so a DATE bounding a DATE-TIME DTSTART admits every
// instance on that day: it is read as the day's last instant in the DTSTART's
// zone, through startWall.
func parseRecurrenceUntil(value string, loc *time.Location, resolve PropertyTimeResolver, startWall wallClockResolver) (time.Time, bool) {
	if startWall != nil && !strings.Contains(value, "T") {
		day, err := time.Parse("20060102", value)
		if err != nil {
			return time.Time{}, false
		}
		return endOfWallClockDay(day, startWall)
	}
	if startWall != nil && !hasZoneSuffix(value) {
		wall, err := ParseDateTime(value)
		if err != nil {
			return time.Time{}, false
		}
		return startWall(wall)
	}
	if !hasZoneSuffix(value) {
		if resolve != nil {
			if parsed, ok := resolve("UNTIL", value); ok {
				return parsed, true
			}
		}
		if loc != nil {
			if parsed, err := ParseDateTimeInLocation(value, loc); err == nil {
				return parsed, true
			}
		}
	}
	parsed, err := ParseDateTime(value)
	return parsed, err == nil
}

// wallClockResolver resolves a wall-clock reading, held as the UTC time with the
// same fields, to the instant it names in one zone.
type wallClockResolver func(wall time.Time) (time.Time, bool)

func wallClockIn(loc *time.Location) wallClockResolver {
	return func(wall time.Time) (time.Time, bool) {
		return WallClockIn(loc, wall), true
	}
}

// dtstartWallClock reads wall-clock readings in the zone a DTSTART content line
// is read in, as resolve reads the line itself. It is nil for a DATE value.
func dtstartWallClock(keyPart, value string, resolve PropertyTimeResolver) wallClockResolver {
	value = strings.TrimSpace(value)
	if !strings.Contains(value, "T") {
		return nil
	}
	if hasZoneSuffix(value) {
		return wallClockIn(time.UTC)
	}
	return func(wall time.Time) (time.Time, bool) {
		return resolve.or(keyPart, wall.Format("20060102T150405"))
	}
}

// endOfWallClockDay is the last instant whose wall-clock reading falls on day.
// The day ends where the next one begins, or one second after its own last
// second, whichever is later: where the clocks spring forward at midnight the
// next day has no 00:00, and a resolver may read that missing reading with the
// offset after the transition, as the time package does, which lands an hour
// before the day actually ends.
func endOfWallClockDay(day time.Time, resolve wallClockResolver) (time.Time, bool) {
	next, ok := resolve(day.AddDate(0, 0, 1))
	if !ok {
		return time.Time{}, false
	}
	lastSecond, ok := resolve(day.Add(24*time.Hour - time.Second))
	if !ok {
		return time.Time{}, false
	}
	if afterLast := lastSecond.Add(time.Second); afterLast.After(next) {
		next = afterLast
	}
	return next.Add(-time.Nanosecond), true
}

func parseIntList(value string, min, max int) ([]int, bool) {
	return parseIntListWithZero(value, min, max, true)
}

func parseIntListAllowNegative(value string, min, max int) ([]int, bool) {
	return parseIntListWithZero(value, min, max, false)
}

func parseIntListWithZero(value string, min, max int, allowZero bool) ([]int, bool) {
	if strings.TrimSpace(value) == "" {
		return nil, true
	}
	seen := make(map[int]struct{})
	var result []int
	for _, part := range strings.Split(value, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(part))
		if err != nil || n < min || n > max || (!allowZero && n == 0) {
			return nil, false
		}
		if _, ok := seen[n]; ok {
			continue
		}
		seen[n] = struct{}{}
		result = append(result, n)
	}
	sort.Ints(result)
	return result, true
}

func parseWeekdayList(value string) ([]weekdaySpecifier, bool) {
	if strings.TrimSpace(value) == "" {
		return nil, true
	}
	seen := make(map[weekdaySpecifier]struct{})
	var result []weekdaySpecifier
	for _, part := range strings.Split(value, ",") {
		part = strings.ToUpper(strings.TrimSpace(part))
		if len(part) < 2 {
			return nil, false
		}
		dayPart := part[len(part)-2:]
		day, ok := parseWeekday(dayPart)
		if !ok {
			return nil, false
		}
		ordinal := 0
		if prefix := part[:len(part)-2]; prefix != "" {
			n, err := strconv.Atoi(prefix)
			if err != nil || n == 0 || n < -53 || n > 53 {
				return nil, false
			}
			ordinal = n
		}
		spec := weekdaySpecifier{Ordinal: ordinal, Day: day}
		if _, ok := seen[spec]; ok {
			continue
		}
		seen[spec] = struct{}{}
		result = append(result, spec)
	}
	return result, true
}

func parseWeekday(value string) (time.Weekday, bool) {
	switch strings.ToUpper(strings.TrimSpace(value)) {
	case "SU":
		return time.Sunday, true
	case "MO":
		return time.Monday, true
	case "TU":
		return time.Tuesday, true
	case "WE":
		return time.Wednesday, true
	case "TH":
		return time.Thursday, true
	case "FR":
		return time.Friday, true
	case "SA":
		return time.Saturday, true
	default:
		return time.Sunday, false
	}
}

func recurrencePeriodStart(dtstart time.Time, rule recurrenceRule) time.Time {
	switch rule.Freq {
	case "SECONDLY":
		return dtstart
	case "MINUTELY":
		return time.Date(dtstart.Year(), dtstart.Month(), dtstart.Day(), dtstart.Hour(), dtstart.Minute(), 0, 0, dtstart.Location())
	case "HOURLY":
		return time.Date(dtstart.Year(), dtstart.Month(), dtstart.Day(), dtstart.Hour(), 0, 0, 0, dtstart.Location())
	case "DAILY":
		return time.Date(dtstart.Year(), dtstart.Month(), dtstart.Day(), 0, 0, 0, 0, dtstart.Location())
	case "WEEKLY":
		return startOfWeek(dtstart, rule.WKST)
	case "MONTHLY":
		return time.Date(dtstart.Year(), dtstart.Month(), 1, 0, 0, 0, 0, dtstart.Location())
	case "YEARLY":
		return time.Date(weekNumberedPeriodYear(dtstart, rule), 1, 1, 0, 0, 0, 0, dtstart.Location())
	default:
		return dtstart
	}
}

// weekNumberedPeriodYear is the year of the yearly period a series begins
// with. A BYWEEKNO period generates its year's numbered weeks, which can begin
// up to three days before January 1 and end up to three days after December
// 31, so the year whose weeks hold a DTSTART near the turn of the year can be
// the one before or after its calendar year. With an INTERVAL of one every year
// is a period of the series, and the series begins with that week-numbering
// year: begun with the calendar year, the period after it would generate the
// DTSTART a second time, counted against COUNT, and the period before it,
// never visited, would leave out the days its last week holds after the
// DTSTART. A longer INTERVAL counts from the DTSTART's calendar year, whose
// neighbours are not periods of the series.
func weekNumberedPeriodYear(dtstart time.Time, rule recurrenceRule) int {
	year := dtstart.Year()
	if len(rule.ByWeekNo) == 0 || rule.Interval > 1 {
		return year
	}
	switch {
	case !dtstart.Before(firstWeekStart(year+1, rule.WKST, dtstart.Location())):
		return year + 1
	case dtstart.Before(firstWeekStart(year, rule.WKST, dtstart.Location())):
		return year - 1
	}
	return year
}

func fastForwardRecurrencePeriod(periodStart, threshold time.Time, rule recurrenceRule) time.Time {
	// No instance exists past UNTIL, so the scan never has to be carried
	// beyond it however far out the requested range starts.
	if rule.Until != nil && rule.Until.Before(threshold) {
		threshold = *rule.Until
	}
	// The jump is measured in Unix seconds rather than through time.Duration,
	// which saturates at roughly 292 years. A DTSTART further back than that --
	// 16010101, the value Exchange writes into a VTIMEZONE observance, is one --
	// would otherwise under-shoot by the whole excess and leave the loop below
	// stepping a second at a time across four centuries.
	if seconds, ok := subDailyRecurrenceSeconds(rule); ok && seconds > 0 && threshold.After(periodStart) {
		if steps := (threshold.Unix() - periodStart.Unix()) / seconds; steps > 0 {
			periodStart = time.Unix(periodStart.Unix()+steps*seconds, int64(periodStart.Nanosecond())).In(periodStart.Location())
		}
	} else if threshold.After(periodStart) {
		// The loop below is bounded by recurrenceScanLimit periods, which a
		// daily, weekly or monthly rule from a distant DTSTART outruns, so the
		// whole periods to the threshold are jumped at once. A caller searching
		// backward from here would otherwise read a period centuries short of
		// the threshold as the one holding it.
		periodStart = jumpCalendarRecurrencePeriods(periodStart, threshold, rule)
	}
	for scanned := 0; scanned < recurrenceScanLimit; scanned++ {
		next := advanceRecurrencePeriod(periodStart, rule)
		// advanceRecurrencePeriod returns its input unchanged on overflow and
		// for a frequency it does not recognize; either leaves the scan unable
		// to move, which without this guard is an unbounded loop.
		if next.After(threshold) || !next.After(periodStart) {
			return periodStart
		}
		periodStart = next
	}
	return periodStart
}

// recurrencePeriodEarliestCandidate is the earliest instant the period
// starting at periodStart can generate. That is the period's start, except for
// a yearly BYWEEKNO period: RFC 5545 numbers weeks from the one holding the
// year's first Thursday, so week 1 can begin up to three days before January 1
// and the period reaches back into the year before it.
func recurrencePeriodEarliestCandidate(periodStart time.Time, rule recurrenceRule) time.Time {
	if rule.Freq != "YEARLY" || len(rule.ByWeekNo) == 0 {
		return periodStart
	}
	if first := firstWeekStart(periodStart.Year(), rule.WKST, periodStart.Location()); first.Before(periodStart) {
		return first
	}
	return periodStart
}

// rewindToPeriodReaching steps back from periodStart, the period holding
// threshold, to the one before it when that one can still generate an instant
// at or after threshold. Only a yearly BYWEEKNO period can: RFC 5545's last
// week of a year can end up to three days after December 31. firstPeriod is the
// period holding the DTSTART, which nothing is generated before.
func rewindToPeriodReaching(periodStart, firstPeriod, threshold time.Time, rule recurrenceRule) time.Time {
	if rule.Freq != "YEARLY" || len(rule.ByWeekNo) == 0 {
		return periodStart
	}
	previous := retreatRecurrencePeriod(periodStart, rule)
	if !previous.Before(periodStart) || previous.Before(firstPeriod) {
		return periodStart
	}
	// A year's weeks run up to the first week of the year after it.
	if firstWeekStart(previous.Year()+1, rule.WKST, previous.Location()).After(threshold) {
		return previous
	}
	return periodStart
}

// jumpCalendarRecurrencePeriods advances periodStart by as many whole periods
// of a daily, weekly, monthly or yearly rule as start at or before threshold,
// counted on the period's calendar so the result stays aligned to INTERVAL.
func jumpCalendarRecurrencePeriods(periodStart, threshold time.Time, rule recurrenceRule) time.Time {
	interval := rule.Interval
	if interval <= 0 {
		interval = 1
	}
	wall := threshold.In(periodStart.Location())
	var steps int
	var jump func(steps int) time.Time
	switch rule.Freq {
	case "DAILY", "WEEKLY":
		days := 1
		if rule.Freq == "WEEKLY" {
			days = 7
		}
		steps = int((civilDayNumber(wall) - civilDayNumber(periodStart)) / int64(days*interval))
		jump = func(steps int) time.Time { return periodStart.AddDate(0, 0, steps*days*interval) }
	case "MONTHLY":
		months := (wall.Year()-periodStart.Year())*12 + int(wall.Month()) - int(periodStart.Month())
		steps = months / interval
		jump = func(steps int) time.Time { return periodStart.AddDate(0, steps*interval, 0) }
	case "YEARLY":
		steps = (wall.Year() - periodStart.Year()) / interval
		jump = func(steps int) time.Time { return periodStart.AddDate(steps*interval, 0, 0) }
	default:
		return periodStart
	}
	if steps <= 0 {
		return periodStart
	}
	jumped := jump(steps)
	// A period begins at the start of its calendar unit, which a zone's
	// transition can move past the threshold on the threshold's own day.
	if jumped.After(threshold) {
		jumped = jump(steps - 1)
	}
	return jumped
}

func subDailyRecurrenceStep(rule recurrenceRule) (time.Duration, bool) {
	seconds, ok := subDailyRecurrenceSeconds(rule)
	if !ok || seconds > int64(time.Duration(1<<63-1)/time.Second) {
		return 0, false
	}
	return time.Duration(seconds) * time.Second, true
}

func subDailyRecurrenceSeconds(rule recurrenceRule) (int64, bool) {
	interval := rule.Interval
	if interval <= 0 {
		interval = 1
	}
	unitSeconds, ok := recurrenceFrequencySeconds(rule.Freq)
	if !ok || int64(interval) > int64(^uint64(0)>>1)/unitSeconds {
		return 0, false
	}
	return int64(interval) * unitSeconds, true
}

func advanceRecurrencePeriod(periodStart time.Time, rule recurrenceRule) time.Time {
	interval := rule.Interval
	if interval <= 0 {
		interval = 1
	}
	switch rule.Freq {
	case "SECONDLY", "MINUTELY", "HOURLY":
		seconds, ok := subDailyRecurrenceSeconds(rule)
		if !ok || periodStart.Unix() > int64(^uint64(0)>>1)-seconds {
			return periodStart
		}
		return time.Unix(periodStart.Unix()+seconds, int64(periodStart.Nanosecond())).In(periodStart.Location())
	case "DAILY":
		return periodStart.AddDate(0, 0, interval)
	case "WEEKLY":
		return periodStart.AddDate(0, 0, 7*interval)
	case "MONTHLY":
		return periodStart.AddDate(0, interval, 0)
	case "YEARLY":
		return periodStart.AddDate(interval, 0, 0)
	default:
		return periodStart
	}
}

// recurrencePeriodWorkLimit bounds the date/time combinations one recurrence
// period may generate.
//
// A period's candidate set is the product of its days and its BYHOUR, BYMINUTE
// and BYSECOND lists, so a yearly rule naming every day, hour, minute and
// second describes about 31.5 million of them. Generating that set to answer a
// question about its first thousand entries is work no answer needs, and the
// PUT validation path reaches it. Exhausting the bound fails closed on
// max-instances, as recurrenceWorkLimit does: a period this large is over any
// instance limit the server advertises.
//
// One day cannot exceed the bound on its own -- 24 hours, 60 minutes and the 61
// second values BYSECOND admits come to 87,840 -- so a rule is only ever refused
// for spanning several such days. The sparse path is therefore always inside the
// bound: it generates a DAILY period, which is one day, or a sub-daily one, which
// is at most an hour.
const recurrencePeriodWorkLimit = 100_000

// recurrenceWorkLimit bounds the generation work one evaluation of a rule may
// do, summed over every period it visits: a period, each day its day list
// holds, and each candidate start those days expand to all cost one unit, and
// the sparse scan pays a unit for each recurrenceDayMask word it reads.
//
// Counting periods alone bounds nothing, because what a period costs is up to
// the rule: a yearly period can offer hundreds of days, so a rule that matches
// rarely or never could spend minutes inside a period count that looks small.
// Running out fails closed, like recurrencePeriodWorkLimit: validation answers
// max-instances and expansion returns ErrRecurrenceScanLimit. The bound sits
// well above what a rule anyone writes needs -- a daily rule for a 29th of
// February that falls on a Monday, with a COUNT of a thousand, runs 28,000
// years and takes about 360,000 -- and holds the costliest evaluation a rule
// can ask for to a few tenths of a second.
const recurrenceWorkLimit = 2_000_000

// recurrenceWork is the budget one evaluation of a rule draws on.
type recurrenceWork struct {
	remaining int
}

func newRecurrenceWork() *recurrenceWork {
	return &recurrenceWork{remaining: recurrenceWorkLimit}
}

// spend takes units from the budget and reports whether it held them. A budget
// that has refused once stays exhausted.
func (w *recurrenceWork) spend(units int) bool {
	if units > w.remaining {
		w.remaining = -1
		return false
	}
	w.remaining -= units
	return true
}

// visitRecurrenceCandidates hands visit each of the period's candidate starts in
// ascending order, stopping as soon as visit returns true. Candidates are
// streamed rather than collected so a caller that needs the first few -- every
// caller with a COUNT, an UNTIL or an instance limit -- pays for the first few.
//
// stopped reports that visit asked to stop. overBudget reports that the period
// exceeded recurrencePeriodWorkLimit or that work ran out, either of which
// leaves the candidate set only partly visited and is never a set the caller
// may treat as complete.
func visitRecurrenceCandidates(periodStart, dtstart time.Time, rule recurrenceRule, work *recurrenceWork, visit func(time.Time) bool) (stopped, overBudget bool) {
	if !work.spend(1) {
		return false, true
	}
	selector := newBySetPosSelector(rule.BySetPos, visit)

	switch rule.Freq {
	case "SECONDLY", "MINUTELY", "HOURLY":
		// At most one hour of minutes and seconds, so the whole period is
		// already small enough to hold.
		candidates := subDailyCandidatesForPeriod(periodStart, dtstart, rule)
		if !work.spend(len(candidates)) {
			return false, true
		}
		candidates = rule.existingLocalTimes(candidates, dtstart)
		sortTimes(candidates)
		for _, candidate := range uniqueTimes(candidates) {
			if selector.offer(candidate) {
				return true, false
			}
			if selector.exhausted() {
				break
			}
		}
		return selector.finish(), false
	}

	var days []time.Time
	switch rule.Freq {
	case "DAILY":
		days = []time.Time{periodStart}
	case "WEEKLY":
		days = weeklyDays(periodStart, dtstart, rule)
	case "MONTHLY":
		days = monthlyDays(periodStart.Year(), periodStart.Month(), dtstart, rule)
	case "YEARLY":
		days = yearlyDays(periodStart.Year(), dtstart, rule)
	default:
		return false, false
	}
	if !work.spend(len(days)) {
		return false, true
	}
	// The day list is bounded by the length of the period, so ordering it here
	// is what lets the times below stream out already sorted. It is deduplicated
	// by calendar date rather than by instant: a DST transition can land two
	// day-list entries on the same date at different instants (e.g. a gap that
	// normalizes one entry onto the previous day), and generating that date's
	// times twice would break both the ascending stream and BYSETPOS's count.
	sortTimes(days)
	days = uniqueDates(days)

	hours := defaultedInts(rule.ByHour, dtstart.Hour())
	minutes := defaultedInts(rule.ByMinute, dtstart.Minute())
	seconds := defaultedInts(rule.BySecond, dtstart.Second())
	perDay := len(hours) * len(minutes) * len(seconds)

	budget := recurrencePeriodWorkLimit
	dayTimes := make([]time.Time, 0, perDay)
	for _, day := range days {
		if !dateMatchesRule(day, rule) {
			continue
		}
		if perDay > budget || !work.spend(perDay) {
			return false, true
		}
		budget -= perDay

		// A day is sorted rather than assumed ordered: a UTC offset change can
		// reorder one day's wall-clock times against each other, and never
		// reaches across the day boundary the outer loop steps over.
		dayTimes = dayTimes[:0]
		for _, hour := range hours {
			for _, minute := range minutes {
				for _, second := range seconds {
					dayTimes = appendValidTime(dayTimes, day.Year(), day.Month(), day.Day(), hour, minute, second, day.Location())
				}
			}
		}
		dayTimes = rule.existingLocalTimes(dayTimes, dtstart)
		sortTimes(dayTimes)
		for _, candidate := range uniqueTimes(dayTimes) {
			if selector.offer(candidate) {
				return true, false
			}
			if selector.exhausted() {
				return selector.finish(), false
			}
		}
	}
	return selector.finish(), false
}

// existingLocalTimes drops the candidates whose wall clock the series' zone
// skips, per recurrenceRule.localTimeExists, reusing the slice. They are dropped
// before BYSETPOS numbers the period, just as appendValidTime drops an invalid
// date before it: §3.3.10 treats the two alike, and the idiom
// BYMONTHDAY=28,29,30,31;BYSETPOS=-1 for "the last day of the month" relies on
// BYSETPOS counting only the instances that exist. The DTSTART is kept whatever
// it names, being the first instance of the set (§3.8.5.3).
func (rule recurrenceRule) existingLocalTimes(candidates []time.Time, dtstart time.Time) []time.Time {
	if rule.localTimeExists == nil {
		return candidates
	}
	kept := candidates[:0]
	for _, candidate := range candidates {
		if candidate.Equal(dtstart) || rule.localTimeExists(candidate) {
			kept = append(kept, candidate)
		}
	}
	return kept
}

// bySetPosSelector applies BYSETPOS to a streamed period.
//
// Without BYSETPOS it is a pass-through. With it, RFC 5545 section 3.3.10
// numbers positions within the period, so a positive one is known as it streams
// past and a negative one needs only the tail: the selector holds
// max(|negative position|) candidates, never the period. Once the largest
// positive position has gone by and no negative position is waiting on the
// tail, nothing further can be selected and the period stops early.
type bySetPosSelector struct {
	positions []int
	visit     func(time.Time) bool

	seen      int
	maxPos    int
	tail      []time.Time
	tailStart int
	selected  []time.Time
	// lastOffered is how the ascending stream is made unique without holding
	// what came before it: a repeat can only ever be the value just seen.
	lastOffered time.Time
	offered     bool
}

func newBySetPosSelector(positions []int, visit func(time.Time) bool) *bySetPosSelector {
	selector := &bySetPosSelector{positions: positions, visit: visit}
	if len(positions) == 0 {
		return selector
	}
	tailLen := 0
	for _, position := range positions {
		switch {
		case position > 0 && position > selector.maxPos:
			selector.maxPos = position
		case position < 0 && -position > tailLen:
			tailLen = -position
		}
	}
	if tailLen > 0 {
		selector.tail = make([]time.Time, tailLen)
	}
	return selector
}

// offer takes the next candidate of the period in ascending order and reports
// whether the caller asked to stop.
func (s *bySetPosSelector) offer(candidate time.Time) bool {
	if s.offered && candidate.Equal(s.lastOffered) {
		return false
	}
	s.lastOffered = candidate
	s.offered = true
	if len(s.positions) == 0 {
		return s.visit(candidate)
	}
	s.seen++
	if s.seen <= s.maxPos {
		for _, position := range s.positions {
			if position == s.seen {
				s.selected = append(s.selected, candidate)
				break
			}
		}
	}
	if len(s.tail) > 0 {
		s.tail[s.tailStart] = candidate
		s.tailStart = (s.tailStart + 1) % len(s.tail)
	}
	return false
}

// finish selects the negative positions, which only the end of the period
// resolves, and hands the whole selection over in ascending order.
func (s *bySetPosSelector) finish() bool {
	if len(s.positions) == 0 {
		return false
	}
	for _, position := range s.positions {
		if position >= 0 {
			continue
		}
		// -1 names the period's last candidate, which is the slot before the
		// ring's next write. A position naming more than the period produced, or
		// more than the ring holds, selects nothing.
		offset := -position
		if offset > s.seen || offset > len(s.tail) {
			continue
		}
		s.selected = append(s.selected, s.tail[(s.tailStart-offset+2*len(s.tail))%len(s.tail)])
	}
	sortTimes(s.selected)
	for _, candidate := range uniqueTimes(s.selected) {
		if s.visit(candidate) {
			return true
		}
	}
	return false
}

// exhausted reports that nothing the period has left to offer can be selected,
// so the caller may stop generating it.
func (s *bySetPosSelector) exhausted() bool {
	return len(s.positions) > 0 && len(s.tail) == 0 && s.seen >= s.maxPos
}

func sortTimes(values []time.Time) {
	slices.SortFunc(values, time.Time.Compare)
}

func subDailyCandidatesForPeriod(periodStart, dtstart time.Time, rule recurrenceRule) []time.Time {
	if !dateMatchesRule(periodStart, rule) {
		return nil
	}
	var candidates []time.Time
	switch rule.Freq {
	case "SECONDLY":
		if timeMatchesRule(periodStart, dtstart, rule) {
			candidates = append(candidates, periodStart)
		}
	case "MINUTELY":
		if !intMatchesIfPresent(periodStart.Hour(), rule.ByHour) || !intMatchesIfPresent(periodStart.Minute(), rule.ByMinute) {
			return nil
		}
		for _, second := range defaultedInts(rule.BySecond, dtstart.Second()) {
			candidates = appendValidTime(candidates, periodStart.Year(), periodStart.Month(), periodStart.Day(), periodStart.Hour(), periodStart.Minute(), second, periodStart.Location())
		}
	case "HOURLY":
		if !intMatchesIfPresent(periodStart.Hour(), rule.ByHour) {
			return nil
		}
		for _, minute := range defaultedInts(rule.ByMinute, dtstart.Minute()) {
			for _, second := range defaultedInts(rule.BySecond, dtstart.Second()) {
				candidates = appendValidTime(candidates, periodStart.Year(), periodStart.Month(), periodStart.Day(), periodStart.Hour(), minute, second, periodStart.Location())
			}
		}
	}
	return candidates
}

func defaultedInts(values []int, fallback int) []int {
	if len(values) > 0 {
		return values
	}
	return []int{fallback}
}

func appendValidTime(values []time.Time, year int, month time.Month, day, hour, minute, second int, loc *time.Location) []time.Time {
	if second == 60 {
		second = 59
	}
	if second < 0 || second > 59 {
		return values
	}
	t := time.Date(year, month, day, hour, minute, second, 0, loc)
	// Date and Clock each read the zone once, which matters in a zone with
	// transitions: this runs for every candidate a rule generates.
	if y, m, d := t.Date(); y != year || m != month || d != day {
		return values
	}
	if h, mi, sec := t.Clock(); h != hour || mi != minute || sec != second {
		return values
	}
	return append(values, t)
}

func timeMatchesRule(t, dtstart time.Time, rule recurrenceRule) bool {
	switch rule.Freq {
	case "SECONDLY":
		return intMatchesIfPresent(t.Hour(), rule.ByHour) &&
			intMatchesIfPresent(t.Minute(), rule.ByMinute) &&
			intMatchesIfPresent(t.Second(), rule.BySecond)
	case "MINUTELY":
		return intMatchesIfPresent(t.Hour(), rule.ByHour) &&
			intMatchesIfPresent(t.Minute(), rule.ByMinute) &&
			intMatchesOrDefault(t.Second(), rule.BySecond, dtstart.Second())
	case "HOURLY":
		return intMatchesIfPresent(t.Hour(), rule.ByHour) &&
			intMatchesOrDefault(t.Minute(), rule.ByMinute, dtstart.Minute()) &&
			intMatchesOrDefault(t.Second(), rule.BySecond, dtstart.Second())
	default:
		return intMatchesOrDefault(t.Hour(), rule.ByHour, dtstart.Hour()) &&
			intMatchesOrDefault(t.Minute(), rule.ByMinute, dtstart.Minute()) &&
			intMatchesOrDefault(t.Second(), rule.BySecond, dtstart.Second())
	}
}

func dateMatchesRule(day time.Time, rule recurrenceRule) bool {
	year, month, monthDay := day.Date()
	return civilDateMatchesRule(year, month, monthDay, day.YearDay(), day.Weekday(), rule)
}

// civilDateMatchesRule reports whether the rule's date filters admit a calendar
// date given by its parts. RFC 5545 §3.3.10 counts a numbered BYDAY weekday
// within the year for a YEARLY rule without BYMONTH, and within the month
// otherwise.
func civilDateMatchesRule(year int, month time.Month, monthDay, yearDay int, weekday time.Weekday, rule recurrenceRule) bool {
	if len(rule.ByMonth) > 0 && !intInSlice(int(month), rule.ByMonth) {
		return false
	}
	if len(rule.ByMonthDay) > 0 && !dayNumberMatches(monthDay, daysInMonth(year, month), rule.ByMonthDay) {
		return false
	}
	if len(rule.ByYearDay) > 0 && !dayNumberMatches(yearDay, daysInYear(year), rule.ByYearDay) {
		return false
	}
	if len(rule.ByDay) == 0 {
		return true
	}
	if rule.Freq == "YEARLY" && len(rule.ByMonth) == 0 {
		return rule.byDaySet.contains(weekday, yearDay, daysInYear(year))
	}
	return rule.byDaySet.contains(weekday, monthDay, daysInMonth(year, month))
}

func intMatchesOrDefault(value int, allowed []int, fallback int) bool {
	if len(allowed) == 0 {
		return value == fallback
	}
	return intInSlice(value, allowed)
}

func intMatchesIfPresent(value int, allowed []int) bool {
	return len(allowed) == 0 || intInSlice(value, allowed)
}

func intInSlice(value int, allowed []int) bool {
	for _, candidate := range allowed {
		if candidate == value {
			return true
		}
	}
	return false
}

func startOfWeek(t time.Time, wkst time.Weekday) time.Time {
	day := time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, t.Location())
	offset := (int(day.Weekday()) - int(wkst) + 7) % 7
	return day.AddDate(0, 0, -offset)
}

func weeklyDays(periodStart, dtstart time.Time, rule recurrenceRule) []time.Time {
	if len(rule.ByDay) == 0 {
		offset := (int(dtstart.Weekday()) - int(rule.WKST) + 7) % 7
		return []time.Time{periodStart.AddDate(0, 0, offset)}
	}
	var days []time.Time
	for _, byDay := range rule.ByDay {
		offset := (int(byDay.Day) - int(rule.WKST) + 7) % 7
		days = append(days, periodStart.AddDate(0, 0, offset))
	}
	return days
}

func monthlyDays(year int, month time.Month, dtstart time.Time, rule recurrenceRule) []time.Time {
	loc := dtstart.Location()
	if len(rule.ByMonth) > 0 && !intInSlice(int(month), rule.ByMonth) {
		return nil
	}
	if len(rule.ByMonthDay) > 0 {
		var days []time.Time
		for _, monthDay := range rule.ByMonthDay {
			if day, ok := resolveMonthDay(year, month, monthDay, loc); ok {
				days = append(days, day)
			}
		}
		return days
	}
	if len(rule.ByDay) > 0 {
		return daysByWeekdayInMonth(year, month, rule.ByDay, loc)
	}
	if day, ok := resolveMonthDay(year, month, dtstart.Day(), loc); ok {
		return []time.Time{day}
	}
	return nil
}

func yearlyDays(year int, dtstart time.Time, rule recurrenceRule) []time.Time {
	loc := dtstart.Location()
	if len(rule.ByWeekNo) > 0 {
		return daysByWeekNoInYear(year, rule.ByWeekNo, rule.WKST, loc)
	}
	if len(rule.ByYearDay) > 0 {
		var days []time.Time
		yearLen := daysInYear(year)
		for _, yearDay := range rule.ByYearDay {
			dayNum := yearDay
			if dayNum < 0 {
				dayNum = yearLen + dayNum + 1
			}
			if dayNum < 1 || dayNum > yearLen {
				continue
			}
			days = append(days, time.Date(year, 1, dayNum, 0, 0, 0, 0, loc))
		}
		return days
	}

	if len(rule.ByMonth) == 0 && len(rule.ByDay) > 0 && len(rule.ByMonthDay) == 0 {
		if hasOrdinalWeekday(rule.ByDay) {
			return daysByWeekdayInYear(year, rule.ByDay, loc)
		}
		var days []time.Time
		for month := 1; month <= 12; month++ {
			days = append(days, monthlyDays(year, time.Month(month), dtstart, recurrenceRule{ByDay: rule.ByDay})...)
		}
		return days
	}

	months := rule.ByMonth
	if len(months) == 0 && len(rule.ByMonthDay) > 0 {
		months = []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12}
	}
	if len(months) == 0 {
		months = []int{int(dtstart.Month())}
	}
	var days []time.Time
	for _, month := range months {
		days = append(days, monthlyDays(year, time.Month(month), dtstart, recurrenceRule{
			ByMonthDay: rule.ByMonthDay,
			ByDay:      rule.ByDay,
		})...)
	}
	return days
}

// daysByWeekNoInYear lists the days of the numbered weeks. A week named both
// from the start and from the end is listed twice; visitRecurrenceCandidates
// orders and deduplicates the day list it generates, as it does for every
// frequency.
func daysByWeekNoInYear(year int, weekNumbers []int, wkst time.Weekday, loc *time.Location) []time.Time {
	weeks := weeksInYear(year, wkst)
	firstWeek := firstWeekStart(year, wkst, loc)
	var days []time.Time
	for _, weekNo := range weekNumbers {
		resolved := weekNo
		if resolved < 0 {
			resolved = weeks + resolved + 1
		}
		if resolved < 1 || resolved > weeks {
			continue
		}
		weekStart := firstWeek.AddDate(0, 0, (resolved-1)*7)
		for day := 0; day < 7; day++ {
			days = append(days, weekStart.AddDate(0, 0, day))
		}
	}
	return days
}

func weeksInYear(year int, wkst time.Weekday) int {
	start := firstWeekStart(year, wkst, time.UTC)
	next := firstWeekStart(year+1, wkst, time.UTC)
	return int(next.Sub(start).Hours() / (24 * 7))
}

func firstWeekStart(year int, wkst time.Weekday, loc *time.Location) time.Time {
	jan1 := time.Date(year, 1, 1, 0, 0, 0, 0, loc)
	weekStart := startOfWeek(jan1, wkst)
	daysInYearInWeek := 7 - int(jan1.Sub(weekStart).Hours()/24)
	if daysInYearInWeek >= 4 {
		return weekStart
	}
	return weekStart.AddDate(0, 0, 7)
}

func hasOrdinalWeekday(specifiers []weekdaySpecifier) bool {
	for _, spec := range specifiers {
		if spec.Ordinal != 0 {
			return true
		}
	}
	return false
}

func daysByWeekdayInYear(year int, specifiers []weekdaySpecifier, loc *time.Location) []time.Time {
	return daysByWeekday(daysInYear(year), civilWeekday(year, time.January, 1), specifiers, func(day int) time.Time {
		return time.Date(year, 1, day, 0, 0, 0, 0, loc)
	})
}

func resolveMonthDay(year int, month time.Month, monthDay int, loc *time.Location) (time.Time, bool) {
	days := daysInMonth(year, month)
	day := monthDay
	if day < 0 {
		day = days + day + 1
	}
	if day < 1 || day > days {
		return time.Time{}, false
	}
	return time.Date(year, month, day, 0, 0, 0, 0, loc), true
}

func daysByWeekdayInMonth(year int, month time.Month, specifiers []weekdaySpecifier, loc *time.Location) []time.Time {
	return daysByWeekday(daysInMonth(year, month), civilWeekday(year, month, 1), specifiers, func(day int) time.Time {
		return time.Date(year, month, day, 0, 0, 0, 0, loc)
	})
}

// daysByWeekday lists the days a BYDAY list names in a month or year of
// dayCount days whose first day is a firstWeekday. Each day number is computed
// rather than found by walking the period, so the cost is the days produced and
// not the specifiers times the length of the period.
func daysByWeekday(dayCount int, firstWeekday time.Weekday, specifiers []weekdaySpecifier, dayAt func(int) time.Time) []time.Time {
	var days []time.Time
	for _, spec := range specifiers {
		first := firstDayNumberOfWeekday(firstWeekday, spec.Day)
		if spec.Ordinal == 0 {
			for day := first; day <= dayCount; day += 7 {
				days = append(days, dayAt(day))
			}
			continue
		}
		day := first + 7*(spec.Ordinal-1)
		if spec.Ordinal < 0 {
			last := first + 7*((dayCount-first)/7)
			day = last + 7*(spec.Ordinal+1)
		}
		if day >= 1 && day <= dayCount {
			days = append(days, dayAt(day))
		}
	}
	return days
}

// firstDayNumberOfWeekday is the 1-based number of the first weekday in a month
// or year whose first day is a firstWeekday.
func firstDayNumberOfWeekday(firstWeekday, weekday time.Weekday) int {
	return 1 + (int(weekday)-int(firstWeekday)+7)%7
}

// civilWeekday is the weekday of a calendar date, which no time zone changes.
func civilWeekday(year int, month time.Month, day int) time.Weekday {
	return time.Date(year, month, day, 0, 0, 0, 0, time.UTC).Weekday()
}

// dayNumberMatches reports whether allowed names the day at position (1-based)
// of a month or year length days long, counted from its start or its end.
// allowed is sorted by parseRecurrenceRule: BYYEARDAY can name 732 values, and
// scanning them for every day a period offers would make the day the unit of a
// quadratic cost.
func dayNumberMatches(position, length int, allowed []int) bool {
	return sortedIntsContain(allowed, position) || sortedIntsContain(allowed, position-length-1)
}

func sortedIntsContain(sorted []int, value int) bool {
	i := sort.SearchInts(sorted, value)
	return i < len(sorted) && sorted[i] == value
}

// weekdaySet is a BYDAY list as a lookup. A day is the nth of its weekday
// counted from either end of its month or year by its position alone, so
// matching it costs the same however many specifiers the list holds -- and a
// deduplicated list can still hold 749.
type weekdaySet struct {
	every [7]bool
	// fromStart and fromEnd carry bit n for the ordinals n and -n; parsing caps
	// an ordinal at 53.
	fromStart [7]uint64
	fromEnd   [7]uint64
}

func newWeekdaySet(specifiers []weekdaySpecifier) weekdaySet {
	var set weekdaySet
	for _, spec := range specifiers {
		switch {
		case spec.Ordinal == 0:
			set.every[spec.Day] = true
		case spec.Ordinal > 0:
			set.fromStart[spec.Day] |= 1 << spec.Ordinal
		default:
			set.fromEnd[spec.Day] |= 1 << -spec.Ordinal
		}
	}
	return set
}

// contains reports whether the day numbered position (1-based) of a month or
// year length days long, falling on weekday, is in the set.
func (s weekdaySet) contains(weekday time.Weekday, position, length int) bool {
	fromStart := (position-1)/7 + 1
	fromEnd := (length-position)/7 + 1
	return s.every[weekday] ||
		s.fromStart[weekday]&(1<<fromStart) != 0 ||
		s.fromEnd[weekday]&(1<<fromEnd) != 0
}

func uniqueTimes(values []time.Time) []time.Time {
	if len(values) < 2 {
		return values
	}
	unique := values[:1]
	for _, value := range values[1:] {
		if value.Equal(unique[len(unique)-1]) {
			continue
		}
		unique = append(unique, value)
	}
	return unique
}

// uniqueDates compacts a sorted slice down to one entry per calendar date. A
// DST transition can map two different instants onto the same date, and
// uniqueTimes' instant comparison would keep both.
func uniqueDates(values []time.Time) []time.Time {
	if len(values) < 2 {
		return values
	}
	unique := values[:1]
	lastYear, lastMonth, lastDay := values[0].Date()
	for _, value := range values[1:] {
		year, month, day := value.Date()
		if year == lastYear && month == lastMonth && day == lastDay {
			continue
		}
		lastYear, lastMonth, lastDay = year, month, day
		unique = append(unique, value)
	}
	return unique
}

func daysInMonth(year int, month time.Month) int {
	return time.Date(year, month+1, 0, 0, 0, 0, 0, time.UTC).Day()
}

func daysInYear(year int) int {
	if time.Date(year, 12, 31, 0, 0, 0, 0, time.UTC).YearDay() == 366 {
		return 366
	}
	return 365
}
