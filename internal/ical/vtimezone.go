package ical

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/jw6ventures/calcard/internal/lru"
)

const (
	// maxCachedVTimezones bounds the generated definitions kept, each a few
	// hundred octets for a zone on stable rules and a few kilobytes for one
	// whose transitions have to be listed.
	maxCachedVTimezones = 1024
	// vtimezoneMargin widens the span a definition covers on both sides, so
	// a wall clock on the span's first or last day, whatever the offset that
	// reads it, falls inside it.
	vtimezoneMargin = 48 * time.Hour
	// minYearlyRuleRun is the shortest run of transitions stated as a rule
	// ending at an UNTIL. A rule takes a component of its own, seven lines,
	// where listing the run takes a line per transition.
	minYearlyRuleRun = 8
	// fixedOffsetOnset is where an observance of a zone that has never
	// changed its offset starts, as VTIMEZONE generators conventionally write
	// it.
	fixedOffsetOnset = "19700101T000000"
)

var vtimezones = newVTimezoneCache(maxCachedVTimezones)

// HostDefinitionProperty is the property VTimezone marks each definition it
// generates with. It says the VTIMEZONE states what the host's time zone
// database says of the zone through the years it was generated for, rather
// than anything a client chose, so a reader may answer for the TZID from that
// database instead and a writer may replace it with a definition covering other
// years. It speaks only for a TZID the host defines; on any other name the
// definition is the only one there is.
const HostDefinitionProperty = "X-CALCARD-HOST-ZONE"

// V11GeneratedDefinition reports whether a VTIMEZONE, its unfolded lines from
// BEGIN through END, has the shape of the definitions the v1.1 editor
// generated from the host's database, which carry no HostDefinitionProperty:
// one TZID and no other property of its own, and for each of a run of
// consecutive years an observance restating on 1 January at midnight the
// offset kept then, followed by one observance for each of that year's
// transitions, in order. Every observance holds exactly DTSTART, TZOFFSETFROM,
// TZOFFSETTO and TZNAME, in that order, without parameters. Clients state
// recurring transitions as rules, and none restates an unchanged offset each 1
// January, so a client's definition is not mistaken for one, and each
// transition follows the marker of its own year, so a definition stating a
// change of name alone on some 1 January, decades before its next transition,
// is not either. A lone
// observance from fixedOffsetOnset is the form generators, VTimezone among
// them, conventionally give a zone that has never changed its offset, and is
// not taken for one either. BEGIN and END
// lines are read as ReadComponents reads them, and the TZID may stand anywhere
// among the observances, so a reader of the component tree, which keeps no
// order between a component's properties and its components, decides alike.
func V11GeneratedDefinition(vtimezone []string) bool {
	const observanceLines = 6
	if len(vtimezone) < 2 || componentLineName(vtimezone[0], "BEGIN:") != "VTIMEZONE" ||
		componentLineName(vtimezone[len(vtimezone)-1], "END:") != "VTIMEZONE" {
		return false
	}
	body := vtimezone[1 : len(vtimezone)-1]
	tzids, observances, year, previous := 0, 0, 0, ""
	for i := 0; i < len(body); {
		if strings.HasPrefix(body[i], "TZID:") {
			tzids++
			i++
			continue
		}
		if i+observanceLines > len(body) {
			return false
		}
		observance := body[i : i+observanceLines]
		i += observanceLines
		observances++
		kind := componentLineName(observance[0], "BEGIN:")
		if (kind != "STANDARD" && kind != "DAYLIGHT") || componentLineName(observance[5], "END:") != kind {
			return false
		}
		start, hasStart := strings.CutPrefix(observance[1], "DTSTART:")
		from, hasFrom := strings.CutPrefix(observance[2], "TZOFFSETFROM:")
		to, hasTo := strings.CutPrefix(observance[3], "TZOFFSETTO:")
		_, hasName := strings.CutPrefix(observance[4], "TZNAME:")
		if !hasStart || !hasFrom || !hasTo || !hasName || len(start) != len("20060102T150405") || start < previous {
			return false
		}
		onset, err := time.Parse("20060102T150405", start)
		if err != nil {
			return false
		}
		if strings.HasSuffix(start, "0101T000000") && from == to {
			markerYear, err := strconv.Atoi(start[:4])
			if err != nil || (year != 0 && markerYear != year+1) {
				return false
			}
			year = markerYear
		} else if year == 0 || !withinADayOfYear(onset, year) {
			return false
		}
		previous = start
	}
	return tzids == 1 && year != 0 && (observances > 1 || previous != fixedOffsetOnset)
}

// withinADayOfYear reports whether onset falls in year, or within a day of
// it: the v1.1 editor wrote a transition that changed only the zone's name as
// its UTC instant, which can lie across New Year from the local year it
// belongs to.
func withinADayOfYear(onset time.Time, year int) bool {
	first := time.Date(year, time.January, 1, 0, 0, 0, 0, time.UTC)
	return !onset.Before(first.AddDate(0, 0, -1)) && onset.Before(first.AddDate(1, 0, 1))
}

// componentLineName is the component name a BEGIN or END line, per prefix,
// opens or closes, as ReadComponents reads it, or "" when line is not one.
func componentLineName(line, prefix string) string {
	if !HasPrefixFold(line, prefix) {
		return ""
	}
	return strings.TrimSpace(strings.ToUpper(line[len(prefix):]))
}

// VTimezone returns a VTIMEZONE component, lines ending in CRLF, that defines
// tzid as the host's time zone database does at every wall clock from first
// through last. Both are wall-clock readings held as the UTC time with the
// same fields, as ParseDateTime returns them, and are taken to whole years
// within CALDAV:min-date-time and CALDAV:max-date-time.
//
// The definition carries HostDefinitionProperty. An RFC 5545 reader of it
// places every reading in the span at the instant the host's zone does. A transition
// is stated in the offset it leaves, as §3.6.5 requires, so a reading the
// transition skips or repeats is placed by the reader's own rule for such
// readings.
//
// The transitions a yearly rule describes are stated as that rule, which runs
// on past last when the host's zone keeps following it through
// CALDAV:max-date-time, as recurrence instances past last may need. Transitions
// no rule describes are listed through last only. Each rule is checked against
// the recurrence expansion readers here use before it is stated. ok is false
// when the host does not define tzid, tzid cannot be written as a TZID value,
// or a rule the zone keeps past CALDAV:max-date-time cannot be stated, since
// listing its transitions would leave readings past that wrong.
func VTimezone(tzid string, first, last time.Time) (text string, ok bool) {
	return vtimezones.generate(tzid, first, last)
}

type vtimezoneKey struct {
	tzid        string
	fromYear    int
	throughYear int
}

type cachedVTimezone struct {
	text string
	ok   bool
}

// vtimezoneCache holds the most recently used generated definitions. A
// definition depends on nothing but the zone and the years it spans, and
// objects naming a zone mostly fall in the same few years.
type vtimezoneCache struct {
	definitions *lru.Cache[vtimezoneKey, cachedVTimezone]
}

func newVTimezoneCache(limit int) *vtimezoneCache {
	// The TZID was read out of a calendar object and shares its memory.
	cloneKey := func(key vtimezoneKey) vtimezoneKey {
		key.tzid = strings.Clone(key.tzid)
		return key
	}
	return &vtimezoneCache{definitions: lru.New[vtimezoneKey, cachedVTimezone](limit, nil, cloneKey)}
}

func (c *vtimezoneCache) generate(tzid string, first, last time.Time) (string, bool) {
	if !validTZIDText(tzid) {
		return "", false
	}
	loc, ok := HostLocation(tzid)
	if !ok {
		return "", false
	}
	minDate, maxDate := DateLimits()
	fromYear := min(max(first.Year(), minDate.Year()), maxDate.Year())
	throughYear := min(max(last.Year(), fromYear), maxDate.Year())
	key := vtimezoneKey{tzid: tzid, fromYear: fromYear, throughYear: throughYear}
	if cached, ok := c.definitions.Get(key); ok {
		return cached.text, cached.ok
	}
	text, ok := buildVTimezone(tzid, loc, fromYear, throughYear, maxDate.Year())
	c.definitions.Add(key, cachedVTimezone{text: text, ok: ok})
	return text, ok
}

// validTZIDText admits the characters zone database names are spelled with,
// none of which a TEXT value has to escape.
func validTZIDText(tzid string) bool {
	if tzid == "" {
		return false
	}
	for _, r := range tzid {
		switch {
		case r >= 'A' && r <= 'Z', r >= 'a' && r <= 'z', r >= '0' && r <= '9':
		case r == '/', r == '_', r == '-', r == '+', r == '.':
		default:
			return false
		}
	}
	return true
}

// zoneTransition is one change of a zone's UTC offset, name or daylight saving
// status.
type zoneTransition struct {
	// at is the instant of the change, in UTC.
	at time.Time
	// from and to are the offsets either side of it, in seconds east of UTC.
	from, to int
	// dst and name describe the time the zone keeps from at onwards.
	dst  bool
	name string
}

// onset is the transition as a VTIMEZONE observance states it: the wall clock
// at the change in the offset it leaves, held as the UTC time with the same
// fields.
func (z zoneTransition) onset() time.Time {
	return z.at.Add(time.Duration(z.from) * time.Second)
}

// hostTransitions lists the changes of loc's time after from through through
// -- of its offset, its name or its daylight saving status -- preceded by the
// change that began the period in force at from when there is one. A change of
// name or daylight saving status alone is a transition between equal offsets:
// it moves no reading, but it is what the zone calls the time from then on.
func hostTransitions(loc *time.Location, from, through time.Time) []zoneTransition {
	var transitions []zoneTransition
	add := func(at time.Time) {
		if sameZoneAcross(loc, at) {
			return
		}
		_, offsetFrom := at.Add(-time.Second).In(loc).Zone()
		after := at.In(loc)
		name, offsetTo := after.Zone()
		transitions = append(transitions, zoneTransition{
			at: at.UTC(), from: offsetFrom, to: offsetTo, dst: after.IsDST(), name: name,
		})
	}
	start, end := from.In(loc).ZoneBounds()
	// The time package can end a period where the zone's rule is followed
	// past its listed transitions without anything changing there.
	for !start.IsZero() && sameZoneAcross(loc, start) {
		start, _ = start.Add(-time.Second).In(loc).ZoneBounds()
	}
	if !start.IsZero() {
		add(start)
	}
	for !end.IsZero() && !end.After(through) {
		add(end)
		previous := end
		if _, end = previous.ZoneBounds(); !end.IsZero() && !end.After(previous) {
			end = nextZoneChange(loc, previous)
		}
	}
	return transitions
}

// sameZoneAcross reports whether loc keeps the same offset, name and daylight
// saving status either side of at.
func sameZoneAcross(loc *time.Location, at time.Time) bool {
	before, after := at.Add(-time.Second).In(loc), at.In(loc)
	beforeName, beforeOffset := before.Zone()
	afterName, afterOffset := after.Zone()
	return beforeName == afterName && beforeOffset == afterOffset && before.IsDST() == after.IsDST()
}

// nextZoneChange finds where the zone in force at t ends when ZoneBounds
// cannot say: the time package, following a zone's rule past its listed
// transitions, ends a leap year's last period at the start of its last day,
// before that day has passed. The answer is the first second of the next day
// whose time differs from t's, or that day's start when none does, which the
// caller asks ZoneBounds about in turn.
func nextZoneChange(loc *time.Location, t time.Time) time.Time {
	zoneAt := func(instant time.Time) (string, int, bool) {
		local := instant.In(loc)
		name, offset := local.Zone()
		return name, offset, local.IsDST()
	}
	name, offset, dst := zoneAt(t)
	low, high := t, t.Add(24*time.Hour)
	if highName, highOffset, highDST := zoneAt(high); highName == name && highOffset == offset && highDST == dst {
		return high
	}
	for high.Sub(low) > time.Second {
		middle := low.Add(high.Sub(low) / 2).Truncate(time.Second)
		if middleName, middleOffset, middleDST := zoneAt(middle); middleName == name && middleOffset == offset && middleDST == dst {
			low = middle
		} else {
			high = middle
		}
	}
	return high
}

// observanceKind identifies the transitions one STANDARD or DAYLIGHT component
// can state: those between the same two offsets into the same time.
type observanceKind struct {
	dst      bool
	from, to int
	name     string
}

func (z zoneTransition) kind() observanceKind {
	return observanceKind{dst: z.dst, from: z.from, to: z.to, name: z.name}
}

// vtimezoneObservance is one STANDARD or DAYLIGHT component. onsets[0] is its
// DTSTART; the rest are RDATEs, or, when rule is set, the starts the rule
// generates from the DTSTART.
type vtimezoneObservance struct {
	kind   observanceKind
	start  time.Time
	onsets []time.Time
	rule   string
}

func buildVTimezone(tzid string, loc *time.Location, fromYear, throughYear, maxYear int) (string, bool) {
	spanStart := time.Date(fromYear, time.January, 1, 0, 0, 0, 0, time.UTC).Add(-vtimezoneMargin)
	spanEnd := time.Date(throughYear+1, time.January, 1, 0, 0, 0, 0, time.UTC).Add(vtimezoneMargin)
	// Transitions are followed past CALDAV:max-date-time, so a rule the zone
	// keeps through it is told apart from one it abandons there.
	horizon := time.Date(maxYear+2, time.February, 1, 0, 0, 0, 0, time.UTC)
	// A zone with no change of offset through the span keeps the one in
	// force at its start, and states that offset alone, from fixedOffsetOnset
	// or from the span's start where the span begins before it.
	fixedOffset := func() string {
		at := spanStart.In(loc)
		name, offset := at.Zone()
		observance := vtimezoneObservance{kind: observanceKind{dst: at.IsDST(), from: offset, to: offset, name: name}}
		if wall := at.Add(time.Duration(offset) * time.Second).UTC(); wall.Format("20060102T150405") < fixedOffsetOnset {
			observance.onsets = []time.Time{time.Date(wall.Year(), wall.Month(), wall.Day(), 0, 0, 0, 0, time.UTC)}
		}
		return renderVTimezone(tzid, []vtimezoneObservance{observance})
	}
	transitions := hostTransitions(loc, spanStart, horizon)
	if len(transitions) == 0 {
		return fixedOffset(), true
	}

	var kinds []observanceKind
	byKind := make(map[observanceKind][]zoneTransition)
	for _, transition := range transitions {
		kind := transition.kind()
		if _, seen := byKind[kind]; !seen {
			kinds = append(kinds, kind)
		}
		byKind[kind] = append(byKind[kind], transition)
	}

	var observances []vtimezoneObservance
	for _, kind := range kinds {
		var listed []zoneTransition
		ofKind := byKind[kind]
		for _, run := range yearlyRuns(ofKind) {
			if run.transitions[0].at.After(spanEnd) {
				break
			}
			last := run.transitions[len(run.transitions)-1]
			openEnded := last.at.Equal(ofKind[len(ofKind)-1].at) && last.onset().Year() > maxYear
			if len(run.transitions) < minYearlyRuleRun && !openEnded {
				listed = append(listed, run.transitions...)
				continue
			}
			observance, ok := ruleObservance(run, openEnded)
			switch {
			case ok:
				observances = append(observances, observance)
			case openEnded:
				// Listing the run would place readings correctly only
				// through the horizon, while the host's zone answers past it.
				return "", false
			default:
				listed = append(listed, run.transitions...)
			}
		}
		listed = slices.DeleteFunc(listed, func(transition zoneTransition) bool { return transition.at.After(spanEnd) })
		if len(listed) == 0 {
			continue
		}
		observance := vtimezoneObservance{kind: kind, start: listed[0].at}
		for _, transition := range listed {
			observance.onsets = append(observance.onsets, transition.onset())
		}
		observances = append(observances, observance)
	}
	if len(observances) == 0 {
		// Every change of offset comes after the span, and the zone had
		// never changed its offset before it.
		return fixedOffset(), true
	}
	slices.SortStableFunc(observances, func(a, b vtimezoneObservance) int { return a.start.Compare(b.start) })
	return renderVTimezone(tzid, observances), true
}

// yearlyRun is a run of transitions of one kind in consecutive years that one
// yearly rule describes, with the forms of that rule they all agree with.
type yearlyRun struct {
	transitions []zoneTransition
	rule        yearlyRule
}

// yearlyRuns splits the transitions of one kind, in order, into the longest
// runs a yearly rule describes.
func yearlyRuns(transitions []zoneTransition) []yearlyRun {
	var runs []yearlyRun
	for _, transition := range transitions {
		rule := yearlyRuleAt(transition.onset())
		if len(runs) > 0 {
			current := &runs[len(runs)-1]
			previous := current.transitions[len(current.transitions)-1].onset()
			if transition.onset().Year() == previous.Year()+1 {
				if merged, ok := current.rule.intersect(rule); ok {
					current.transitions = append(current.transitions, transition)
					current.rule = merged
					continue
				}
			}
		}
		runs = append(runs, yearlyRun{transitions: []zoneTransition{transition}, rule: rule})
	}
	return runs
}

// yearlyRule is the set of RFC 5545 yearly rules that place a transition on
// the wall clock it falls at: the same month and time of day, and the day
// picked by date, by ordinal weekday, by last weekday, or by a weekday within a
// seven-day window of month days.
type yearlyRule struct {
	month     time.Month
	timeOfDay time.Duration
	// monthDay is the day of the month, or 0 when the day moves.
	monthDay int
	// weekday is the day of the week the forms below pick, which are void
	// when weekdays reports that it moves.
	weekday  time.Weekday
	weekdays bool
	// ordinal is the BYDAY ordinal, 1 to 4, that names the day, or 0.
	ordinal int
	// last reports that the day is the month's last such weekday.
	last bool
	// windowFrom and windowTo bound the first day of a seven-day BYMONTHDAY
	// window holding the day; the set is empty when windowFrom > windowTo.
	windowFrom, windowTo int
}

func yearlyRuleAt(onset time.Time) yearlyRule {
	day := onset.Day()
	monthLength := daysInMonth(onset.Year(), onset.Month())
	rule := yearlyRule{
		month:     onset.Month(),
		timeOfDay: onset.Sub(time.Date(onset.Year(), onset.Month(), day, 0, 0, 0, 0, time.UTC)),
		monthDay:  day,
		weekday:   onset.Weekday(),
		weekdays:  true,
		last:      day > monthLength-7,
		// A window has to fit the month in every year, so February's ends
		// on the 28th.
		windowFrom: max(1, day-6),
		windowTo:   min(day, shortestMonthLength(onset.Month())-6),
	}
	if day <= 28 {
		rule.ordinal = (day-1)/7 + 1
	}
	return rule
}

func (r yearlyRule) intersect(other yearlyRule) (yearlyRule, bool) {
	if r.month != other.month || r.timeOfDay != other.timeOfDay {
		return yearlyRule{}, false
	}
	if r.monthDay != other.monthDay {
		r.monthDay = 0
	}
	if r.weekday != other.weekday {
		r.weekdays = false
	}
	if r.ordinal != other.ordinal {
		r.ordinal = 0
	}
	r.last = r.last && other.last
	r.windowFrom = max(r.windowFrom, other.windowFrom)
	r.windowTo = min(r.windowTo, other.windowTo)
	return r, r.monthDay != 0 || r.weekdays && (r.ordinal != 0 || r.last || r.windowFrom <= r.windowTo)
}

// recurrence is the RRULE value placing the run, without its UNTIL. The
// weekday forms are preferred, being how zone rules are written.
func (r yearlyRule) recurrence() string {
	weekday := strings.ToUpper(r.weekday.String()[:2])
	prefix := "FREQ=YEARLY;BYMONTH=" + strconv.Itoa(int(r.month))
	switch {
	case r.weekdays && r.last:
		return prefix + ";BYDAY=-1" + weekday
	case r.weekdays && r.ordinal != 0:
		return prefix + ";BYDAY=" + strconv.Itoa(r.ordinal) + weekday
	case r.weekdays && r.windowFrom <= r.windowTo:
		days := make([]string, 7)
		for i := range days {
			days[i] = strconv.Itoa(r.windowFrom + i)
		}
		return prefix + ";BYMONTHDAY=" + strings.Join(days, ",") + ";BYDAY=" + weekday
	}
	return prefix + ";BYMONTHDAY=" + strconv.Itoa(r.monthDay)
}

// ruleObservance states a run as a yearly rule, and reports whether the
// recurrence expansion the report matcher reads observances with generates
// exactly the run's onsets from it. An open-ended rule is the run continuing
// past the last transition listed; otherwise UNTIL ends it at the run's last
// onset.
func ruleObservance(run yearlyRun, openEnded bool) (vtimezoneObservance, bool) {
	first, last := run.transitions[0], run.transitions[len(run.transitions)-1]
	rule := run.rule.recurrence()
	if !openEnded {
		// RFC 5545 states UNTIL in UTC, while a reader comparing it with the
		// onsets on their own scale reads the onset as a wall clock. Either
		// reading of the later of the two keeps the last onset and nothing
		// after it, since the next onset of the rule is a year away.
		until := last.at
		if onset := last.onset(); onset.After(until) {
			until = onset
		}
		rule += ";UNTIL=" + until.Format("20060102T150405") + "Z"
	}
	onsets := make([]time.Time, len(run.transitions))
	for i, transition := range run.transitions {
		onsets[i] = transition.onset()
	}
	// No yearly onset follows another by less than a year less a week, so
	// listing that far past the last onset shows whether the rule ends there.
	through := onsets[len(onsets)-1].AddDate(0, 11, 0)
	lister := NewRecurrenceStartLister(onsets[0], rule)
	if !lister.ListThrough(through, len(onsets)+1) || !slices.EqualFunc(lister.starts, onsets, time.Time.Equal) {
		return vtimezoneObservance{}, false
	}
	return vtimezoneObservance{kind: first.kind(), start: first.at, onsets: onsets[:1], rule: rule}, true
}

func shortestMonthLength(month time.Month) int {
	if month == time.February {
		return 28
	}
	return daysInMonth(2001, month)
}

func renderVTimezone(tzid string, observances []vtimezoneObservance) string {
	var b strings.Builder
	b.WriteString("BEGIN:VTIMEZONE\r\nTZID:")
	b.WriteString(tzid)
	b.WriteString("\r\n" + HostDefinitionProperty + ":TRUE\r\n")
	for _, observance := range observances {
		component := "STANDARD"
		if observance.kind.dst {
			component = "DAYLIGHT"
		}
		dtstart := fixedOffsetOnset
		if len(observance.onsets) > 0 {
			dtstart = observance.onsets[0].Format("20060102T150405")
		}
		fmt.Fprintf(&b, "BEGIN:%s\r\nDTSTART:%s\r\nTZOFFSETFROM:%s\r\nTZOFFSETTO:%s\r\n",
			component, dtstart, formatUTCOffset(observance.kind.from), formatUTCOffset(observance.kind.to))
		if validTZNameText(observance.kind.name) {
			b.WriteString("TZNAME:")
			b.WriteString(observance.kind.name)
			b.WriteString("\r\n")
		}
		if observance.rule != "" {
			b.WriteString("RRULE:")
			b.WriteString(observance.rule)
			b.WriteString("\r\n")
		}
		for _, onset := range observance.onsets[min(1, len(observance.onsets)):] {
			b.WriteString("RDATE:")
			b.WriteString(onset.Format("20060102T150405"))
			b.WriteString("\r\n")
		}
		fmt.Fprintf(&b, "END:%s\r\n", component)
	}
	b.WriteString("END:VTIMEZONE\r\n")
	return b.String()
}

// formatUTCOffset writes an RFC 5545 §3.3.14 UTC-OFFSET, with seconds only
// where the offset has them, as local mean time does.
func formatUTCOffset(seconds int) string {
	sign := byte('+')
	if seconds < 0 {
		sign, seconds = '-', -seconds
	}
	offset := fmt.Sprintf("%c%02d%02d", sign, seconds/3600, seconds/60%60)
	if seconds%60 != 0 {
		offset += fmt.Sprintf("%02d", seconds%60)
	}
	return offset
}

// validTZNameText admits the abbreviations the zone database uses, which are
// letters, or a signed number for a zone without one.
func validTZNameText(name string) bool {
	if name == "" {
		return false
	}
	for _, r := range name {
		if !(r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '+' || r == '-') {
			return false
		}
	}
	return true
}
