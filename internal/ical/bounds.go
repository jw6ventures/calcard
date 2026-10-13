package ical

import (
	"strings"
	"time"
)

// RecurrenceBounds describes conservative SQL-pushdown bounds for recurring
// calendar components. Unknown bounds must be mapped to open-ended sentinels by
// callers so they never exclude an occurrence that exact expansion could match.
type RecurrenceBounds struct {
	Start        *time.Time
	Until        *time.Time
	Recurring    bool
	StartUnknown bool
	UntilUnknown bool
}

// RecurrenceStartSentinel and RecurrenceUntilSentinel are the open-ended values
// a caller substitutes for a bound RecurrenceBounds could not compute, so a row
// the columns cannot bound stays a candidate for every range and exact
// expansion makes the decision.
var (
	RecurrenceStartSentinel = time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
	RecurrenceUntilSentinel = time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)
)

// ConservativeRecurrenceBounds computes bounds for top-level VEVENT, VTODO and
// VJOURNAL components without under-approximating their recurrence windows.
func ConservativeRecurrenceBounds(raw string) RecurrenceBounds {
	return ConservativeRecurrenceBoundsFromUnfoldedLines(UnfoldLines(raw))
}

// ConservativeRecurrenceBoundsFromUnfoldedLines computes the same safe bounds
// without unfolding the payload again when a caller already performed that
// step as part of a broader calendar analysis.
func ConservativeRecurrenceBoundsFromUnfoldedLines(lines []string) RecurrenceBounds {
	return ConservativeRecurrenceBoundsOf(NewObjectTimes(lines))
}

// ConservativeRecurrenceBoundsOf computes the same safe bounds for the object
// times reads, resolving its dates as times does.
func ConservativeRecurrenceBoundsOf(times *ObjectTimes) RecurrenceBounds {
	components := recurringComponentsFromLines(times)
	var bounds RecurrenceBounds
	for _, component := range components {
		if !component.hasRecurrence() {
			continue
		}
		bounds.Recurring = true

		starts := component.recurrenceStarts()
		if len(starts) == 0 || component.unsafeStart {
			bounds.StartUnknown = true
		}
		for _, start := range starts {
			setMinimumTime(&bounds.Start, start)
		}

		if component.hasRDate || component.unsafeUntil || component.rangeThisAndFuture {
			bounds.UntilUnknown = true
		}

		if component.recurrenceID != nil && component.rrule == "" && !component.hasRDate {
			until := component.overrideUntil()
			if until == nil {
				bounds.UntilUnknown = true
			} else {
				setMaximumTime(&bounds.Until, *until)
			}
		}

		if component.rrule == "" {
			continue
		}
		if component.dtstart == nil {
			bounds.UntilUnknown = true
			continue
		}
		skipped := func() (localTimeGaps, bool) {
			return seriesLocalTimeGaps(times, component.dtstartKeyPart, component.dtstartValue)
		}
		until, known := boundedRecurrenceUntil(*component.dtstart, component.dtend, component.duration, component.allDay, component.rrule, component.dtstartWall, skipped)
		if !known {
			bounds.UntilUnknown = true
			continue
		}
		if until != nil {
			setMaximumTime(&bounds.Until, *until)
		}
	}
	return bounds
}

func setMinimumTime(target **time.Time, value time.Time) {
	if target == nil || value.IsZero() {
		return
	}
	if *target == nil || value.Before(**target) {
		copy := value
		*target = &copy
	}
}

func setMaximumTime(target **time.Time, value time.Time) {
	if target == nil || value.IsZero() {
		return
	}
	if *target == nil || value.After(**target) {
		copy := value
		*target = &copy
	}
}

type recurringComponent struct {
	dtstart            *time.Time
	dtstartWall        wallClockResolver
	dtstartKeyPart     string
	dtstartValue       string
	dtend              *time.Time
	duration           *time.Duration
	recurrenceID       *time.Time
	dtstartSeen        bool
	dtendSeen          bool
	durationSeen       bool
	recurrenceIDSeen   bool
	rruleSeen          bool
	rrule              string
	hasRDate           bool
	rdateStarts        []time.Time
	unsafeStart        bool
	unsafeUntil        bool
	allDay             bool
	rangeThisAndFuture bool
}

func (c recurringComponent) hasRecurrence() bool {
	return c.rrule != "" || c.hasRDate || c.recurrenceID != nil
}

func (c recurringComponent) recurrenceStarts() []time.Time {
	starts := make([]time.Time, 0, 2+len(c.rdateStarts))
	if c.dtstart != nil {
		starts = append(starts, *c.dtstart)
	}
	if c.recurrenceID != nil && c.dtstart == nil {
		starts = append(starts, *c.recurrenceID)
	}
	return append(starts, c.rdateStarts...)
}

func (c recurringComponent) overrideUntil() *time.Time {
	if c.recurrenceID == nil {
		return nil
	}
	start := c.recurrenceID
	if c.dtstart != nil {
		start = c.dtstart
	}
	if c.dtend == nil {
		if c.duration == nil || *c.duration <= 0 {
			return nil
		}
		until := start.Add(*c.duration)
		if c.allDay {
			// EventEnd reads it in the whole days it reaches into.
			until = start.AddDate(0, 0, DurationDays(*c.duration))
		}
		return &until
	}
	if !c.dtend.After(*start) {
		return nil
	}
	until := *c.dtend
	return &until
}

func recurringComponentsFromLines(times *ObjectTimes) []recurringComponent {
	parsedComponents := times.recurringComponents()
	components := make([]recurringComponent, 0, len(parsedComponents))
	for _, parsed := range parsedComponents {
		current := recurringComponent{}
		for _, line := range parsed.malformedProperties {
			if strings.EqualFold(PropertyName(strings.TrimSpace(line)), "RDATE") {
				current.hasRDate = true
				current.unsafeStart = true
				current.unsafeUntil = true
			}
		}
		for _, parsedProperty := range parsed.properties {
			keyPart := strings.TrimSpace(parsedProperty.KeyPart)
			value := strings.TrimSpace(parsedProperty.Value)
			property := strings.ToUpper(PropertyName(keyPart))
			switch property {
			case "DTSTART":
				if current.dtstartSeen {
					current.unsafeStart = true
					current.unsafeUntil = true
					continue
				}
				current.dtstartSeen = true
				if parsed, ok := times.ResolvePropertyTime(keyPart, value); ok {
					current.dtstart = timePointer(parsed)
					current.dtstartWall = dtstartWallClock(keyPart, value, times.ResolvePropertyTime)
					current.dtstartKeyPart, current.dtstartValue = keyPart, value
					current.allDay = len(value) == len("20060102") || PropertyParamEquals(keyPart, "VALUE", "DATE")
				}
			case "DTEND":
				if current.dtendSeen {
					current.unsafeUntil = true
					continue
				}
				current.dtendSeen = true
				if parsed, ok := times.ResolvePropertyTime(keyPart, value); ok {
					current.dtend = timePointer(parsed)
				}
			case "DURATION":
				if current.durationSeen {
					current.unsafeUntil = true
					continue
				}
				current.durationSeen = true
				if duration, ok := ParseDuration(value); ok && duration > 0 {
					current.duration = &duration
				}
			case "RECURRENCE-ID":
				if current.recurrenceIDSeen {
					current.unsafeStart = true
					current.unsafeUntil = true
					continue
				}
				current.recurrenceIDSeen = true
				if parsed, ok := times.ResolvePropertyTime(keyPart, value); ok {
					current.recurrenceID = timePointer(parsed)
					current.rangeThisAndFuture = PropertyParamEquals(keyPart, "RANGE", "THISANDFUTURE")
				} else {
					current.unsafeStart = true
					current.unsafeUntil = true
				}
			case "RRULE":
				if current.rruleSeen {
					current.unsafeUntil = true
					continue
				}
				current.rruleSeen = true
				current.rrule = value
			case "RDATE":
				current.hasRDate = true
				starts, ok := rdateStarts(times, keyPart, value)
				current.rdateStarts = append(current.rdateStarts, starts...)
				if !ok {
					current.unsafeStart = true
				}
			}
		}
		components = append(components, current)
	}
	return components
}

func rdateStarts(times *ObjectTimes, keyPart, value string) ([]time.Time, bool) {
	var starts []time.Time
	ok := true
	for _, part := range strings.Split(value, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if strings.Contains(part, "/") {
			part = strings.TrimSpace(strings.SplitN(part, "/", 2)[0])
		}
		if parsed, parsedOK := times.ResolvePropertyTime(keyPart, part); parsedOK {
			starts = append(starts, parsed)
		} else {
			ok = false
		}
	}
	return starts, ok
}

func isRecurringComponentName(name string) bool {
	switch strings.ToUpper(strings.TrimSpace(name)) {
	case "VEVENT", "VTODO", "VJOURNAL":
		return true
	default:
		return false
	}
}

func timePointer(value time.Time) *time.Time {
	copy := value
	return &copy
}

const recurrenceBoundPad = 24 * time.Hour
const recurrenceBoundDefaultDuration = time.Hour

// boundedRecurrenceUntil bounds the end of the last instance of the recurrence
// set a DTSTART and rule describe.
// skipped lists the local times the series' zone skips, which RFC 5545 §3.3.10
// leaves out of a counted series without counting them; it reports false when
// the zone's definition cannot say.
func boundedRecurrenceUntil(dtstart time.Time, dtend *time.Time, durationProperty *time.Duration, allDay bool, rrule string, dtstartWall wallClockResolver, skipped func() (localTimeGaps, bool)) (*time.Time, bool) {
	duration := recurrenceBoundDefaultDuration
	durationResolved := false
	if dtend != nil {
		if candidate := dtend.Sub(dtstart); candidate > 0 {
			duration = candidate
			durationResolved = true
		}
	}
	if !durationResolved {
		if durationProperty != nil && *durationProperty > 0 {
			duration = *durationProperty
			if allDay {
				// EventEnd reads it in the whole days it reaches into.
				duration = time.Duration(DurationDays(duration)) * 24 * time.Hour
			}
		} else if allDay {
			duration = 24 * time.Hour
		}
	}

	rule, ok := parseRecurrenceRule(rrule, dtstart.Location(), nil, dtstartWall)
	if !ok {
		return nil, false
	}
	if rule.Until != nil {
		// The DTSTART is the first instance even when UNTIL falls before it
		// (RFC 5545 §3.8.5.3).
		last := *rule.Until
		if dtstart.After(last) {
			last = dtstart
		}
		until := last.Add(duration)
		return &until, true
	}
	if rule.Count == 0 || hasRRuleByPart(rrule) {
		return nil, false
	}

	steps := (rule.Count - 1) * rule.Interval
	var lastStart time.Time
	var stepDays int
	switch rule.Freq {
	case "DAILY":
		lastStart = dtstart.AddDate(0, 0, steps)
		stepDays = rule.Interval
	case "WEEKLY":
		lastStart = dtstart.AddDate(0, 0, 7*steps)
		stepDays = 7 * rule.Interval
	default:
		return nil, false
	}
	gaps, ok := skipped()
	if !ok {
		return nil, false
	}
	if gaps != nil {
		if lastStart, ok = extendPastSkippedLocalTimes(dtstart, lastStart, stepDays, gaps); !ok {
			return nil, false
		}
	}
	until := lastStart.Add(duration + recurrenceBoundPad)
	return &until, true
}

// LocalTimeGap is a stretch of local time a zone skips: the wall clocks from
// Start, held as UTC fields, up to Start+Length name no instant.
type LocalTimeGap struct {
	Start  time.Time
	Length time.Duration
}

// localTimeGaps lists the gaps of one zone that start after after and at or
// before through, both wall clocks held as UTC fields. Successive calls ask
// about later windows. ok is false when the zone's definition cannot be
// followed that far.
type localTimeGaps func(after, through time.Time) (gaps []LocalTimeGap, ok bool)

// extendPastSkippedLocalTimes moves last, the COUNT-th start of a series
// repeating every stepDays days at one time of day, past the instances the
// zone's gaps take out of it, each of which carries the series a step further.
// A gap is shorter than two days, so it takes out at most ceil(length/step) of
// those instances; counting every gap as taking that many, whether or not the
// series' time of day falls inside it, can only move the bound later. The gaps
// the extension itself runs into are walked in turn, up to CALDAV:max-date-time,
// past which no range reaches.
//
// first and last are compared with the gaps' wall clocks with a margin of a day
// either side, which covers the UTC offset either may have been read with.
func extendPastSkippedLocalTimes(first, last time.Time, stepDays int, gaps localTimeGaps) (time.Time, bool) {
	_, maxDate := DateLimits()
	after := first.Add(-3 * 24 * time.Hour)
	for !last.After(maxDate) {
		through := last.Add(2 * 24 * time.Hour)
		found, ok := gaps(after, through)
		if !ok {
			return time.Time{}, false
		}
		if len(found) == 0 {
			return last, true
		}
		for _, gap := range found {
			last = last.AddDate(0, 0, stepDays*instancesInGap(gap.Length, stepDays))
		}
		after = through
	}
	return last, true
}

// instancesInGap bounds how many instances of a series repeating every
// stepDays days at one time of day a gap of length can hold.
func instancesInGap(length time.Duration, stepDays int) int {
	stepHours := int64(stepDays) * 24
	gapHours := int64((length + time.Hour - 1) / time.Hour)
	return int((gapHours + stepHours - 1) / stepHours)
}

// seriesLocalTimeGaps is the source of the gaps a series whose DTSTART is the
// content line keyPart:value skips, read from the definition the expansion
// reads its TZID through: the VTIMEZONE the object ships, else the host's zone
// of that name. gaps is nil for a series no zone can skip an instance of -- a
// UTC, DATE or floating DTSTART, or a TZID neither defines, all of which the
// expansion places without one.
func seriesLocalTimeGaps(times *ObjectTimes, keyPart, value string) (gaps localTimeGaps, ok bool) {
	tzid, zoned := localDateTimeTZID(keyPart, value)
	if !zoned {
		return nil, true
	}
	zone := times.zone(tzid)
	switch {
	case zone.object != nil:
		return zone.object, true
	case zone.unreadable:
		return nil, false
	case zone.host != nil:
		return hostLocalTimeGaps(zone.host), true
	}
	return nil, true
}

// localDateTimeTZID is the TZID of a DATE-TIME content line that names a local
// time in a zone, as opposed to a DATE, a UTC value, or a floating one.
func localDateTimeTZID(keyPart, value string) (string, bool) {
	value = strings.TrimSpace(value)
	if !strings.Contains(value, "T") || hasZoneSuffix(value) {
		return "", false
	}
	return PropertyParam(keyPart, "TZID")
}

// tzidDefinition is the definition a TZID in an object is read through. The
// order is the DAV layer's matcher's: the VTIMEZONE the object ships, when it
// holds an observance and every observance can be read, else the host's zone
// of that name. RFC 4791 §4.1 makes the object carry a definition of every
// TZID it uses, so that definition wins over the host's even when the two
// disagree, unless the server wrote it from the host's database for a zone
// the host defines (TimezoneComponent.HostDefinition), so it speaks for that
// database.
type tzidDefinition struct {
	// object lists the gaps of the object's own definition, when it is the
	// one that answers.
	object localTimeGaps
	// observances is that definition, and onsets the listings its rules
	// are read through.
	observances *IndexedTimezoneObservances
	onsets      ObservanceOnsets
	// tzid names the zone, which the host's database is asked about only
	// where the object's definition cannot answer.
	tzid string
	// unreadable reports that the object ships observances that cannot be
	// read, which leaves the host's zone answering for the TZID.
	unreadable bool
	// host is the host's zone of that name, when the object's definition
	// does not answer at all.
	host *time.Location
}

func resolveTZID(lines []string, tzid string) tzidDefinition {
	return resolveTZIDIn(objectTimezones(lines), tzid, nil, NewObservanceWork())
}

// resolveTZIDIn is resolveTZID for an object whose VTIMEZONEs objectTimezones
// indexed as timezones, reading the definition's rules through onsets, or
// through listings of its own when onsets is nil, and walking a rule neither
// answers on work, the budget of the object's reading.
func resolveTZIDIn(timezones map[string]objectTimezone, tzid string, onsets ObservanceOnsets, work *ObservanceWork) tzidDefinition {
	timezone, defined := timezones[tzid]
	if timezone.hostGenerated {
		if loc, ok := HostLocation(tzid); ok {
			return tzidDefinition{tzid: tzid, host: loc}
		}
	}
	components := timezone.observances
	if observances, ok := ReadTimezoneObservances(components); ok {
		if onsets == nil {
			onsets = onsetListings{}
		}
		indexed := IndexTimezoneObservances(observances, work)
		return tzidDefinition{
			object: func(after, through time.Time) ([]LocalTimeGap, bool) {
				return indexed.Gaps(after, through, onsets)
			},
			observances: indexed,
			onsets:      onsets,
			tzid:        tzid,
		}
	}
	loc, _ := HostLocation(tzid)
	return tzidDefinition{tzid: tzid, unreadable: defined && len(components) > 0, host: loc}
}

// skips reports whether the object's own definition of the zone skips wall,
// per TimezoneObservances.Skips. known is false for a zone the object does not
// define.
func (z tzidDefinition) skips(wall time.Time) (skipped, known bool) {
	return z.observances.Skips(wall, z.onsets)
}

// instant resolves a wall clock in the object's own definition to the instant
// it names, as the DAV layer's matcher resolves it: with the offset that
// definition reads the wall clock with, or, where its rules cannot be followed
// that far, as the host's zone of that name reads it, else as UTC.
func (z tzidDefinition) instant(wall time.Time) time.Time {
	if offset, ok := z.observances.OffsetAtWall(wall, z.onsets); ok {
		return wall.Add(-offset).UTC()
	}
	host := z.host
	if host == nil {
		host, _ = HostLocation(z.tzid)
	}
	if host != nil {
		return WallClockIn(host, wall).UTC()
	}
	return wall
}

// hostLocalTimeGaps lists the gaps of a zone the host defines.
func hostLocalTimeGaps(loc *time.Location) localTimeGaps {
	return func(after, through time.Time) ([]LocalTimeGap, bool) {
		var gaps []LocalTimeGap
		// A transition's instant lies within a day of the wall clock it
		// starts at.
		for _, transition := range hostTransitions(loc, after.Add(-24*time.Hour), through.Add(24*time.Hour)) {
			if transition.to <= transition.from {
				continue
			}
			if start := transition.onset(); start.After(after) && !start.After(through) {
				gaps = append(gaps, LocalTimeGap{Start: start, Length: time.Duration(transition.to-transition.from) * time.Second})
			}
		}
		return gaps, true
	}
}

// objectTimezone is one VTIMEZONE beneath an object's VCALENDAR: the content
// lines of each of its observances, and whether the server wrote it, as
// TimezoneComponent.HostDefinition reports.
type objectTimezone struct {
	observances   [][]PropertyValue
	hostGenerated bool
}

// TimezoneComponent is one VTIMEZONE beneath an object's VCALENDAR, as
// TimezoneComponents reads it.
type TimezoneComponent struct {
	// TZID is the value of its first TZID property, less surrounding white
	// space, or "" when it has none.
	TZID string
	// HostDefinition reports that the server wrote it from the host's time
	// zone database: it carries HostDefinitionProperty, as VTimezone's
	// definitions do, or it has the shape V11GeneratedDefinition recognises.
	// It speaks for that database wherever the host defines TZID.
	HostDefinition bool
	// Start and End delimit it within the lines it was read from: lines[Start]
	// is its BEGIN line, and lines[End-1] the line that closes it.
	Start, End int

	named       bool
	observances [][]PropertyValue
}

// TimezoneComponents lists, in order, every VTIMEZONE directly beneath the
// VCALENDAR of the object whose unfolded content lines are lines, read as
// ReadComponents reads a stored object, a later definition of a TZID included.
// A property name is matched without regard to case or surrounding white
// space, as the DAV layer reads it.
func TimezoneComponents(lines []string) []TimezoneComponent {
	type component struct {
		timezone *TimezoneComponent
		// observance is the index of the observance it is, within its
		// VTIMEZONE's, for a STANDARD or DAYLIGHT.
		observance int
	}
	var timezones []*TimezoneComponent
	root := &component{}
	_, _ = readComponents(lines, false,
		func(parent *component, name string, at int) (*component, error) {
			switch {
			case parent == nil:
				return root, nil
			case parent == root && name == "VTIMEZONE":
				timezone := &TimezoneComponent{Start: at}
				timezones = append(timezones, timezone)
				return &component{timezone: timezone, observance: -1}, nil
			case parent.timezone != nil && parent.observance < 0 && (name == "STANDARD" || name == "DAYLIGHT"):
				parent.timezone.observances = append(parent.timezone.observances, nil)
				return &component{timezone: parent.timezone, observance: len(parent.timezone.observances) - 1}, nil
			}
			return nil, nil
		},
		func(current *component, line string) error {
			if current == root {
				return nil
			}
			keyPart, value, ok := SplitContentLine(line)
			if !ok {
				return nil
			}
			timezone := current.timezone
			if current.observance >= 0 {
				timezone.observances[current.observance] = append(timezone.observances[current.observance], PropertyValue{KeyPart: keyPart, Value: value})
				return nil
			}
			switch name := strings.TrimSpace(PropertyName(keyPart)); {
			case strings.EqualFold(name, "TZID") && !timezone.named:
				timezone.TZID, timezone.named = strings.TrimSpace(value), true
			case strings.EqualFold(name, HostDefinitionProperty):
				timezone.HostDefinition = true
			}
			return nil
		},
		func(closed *component, end int) {
			if closed.timezone != nil && closed.observance < 0 {
				closed.timezone.End = end
			}
		})
	listed := make([]TimezoneComponent, len(timezones))
	for i, timezone := range timezones {
		if !timezone.HostDefinition && timezone.End > timezone.Start {
			timezone.HostDefinition = V11GeneratedDefinition(lines[timezone.Start:timezone.End])
		}
		listed[i] = *timezone
	}
	return listed
}

// objectTimezones indexes the VTIMEZONEs beneath the object's VCALENDAR by
// their first TZID. A TZID is resolved through the first VTIMEZONE it names, as
// the DAV layer resolves it. Reading every definition in one pass keeps an
// object naming many zones from being read once per zone.
func objectTimezones(lines []string) map[string]objectTimezone {
	timezones := TimezoneComponents(lines)
	indexed := make(map[string]objectTimezone, len(timezones))
	for _, timezone := range timezones {
		if !timezone.named {
			continue
		}
		if _, earlier := indexed[timezone.TZID]; earlier {
			continue
		}
		indexed[timezone.TZID] = objectTimezone{observances: timezone.observances, hostGenerated: timezone.HostDefinition}
	}
	return indexed
}

func hasRRuleByPart(rrule string) bool {
	for _, part := range strings.Split(rrule, ";") {
		key := strings.TrimSpace(strings.SplitN(part, "=", 2)[0])
		if strings.HasPrefix(strings.ToUpper(key), "BY") {
			return true
		}
	}
	return false
}
