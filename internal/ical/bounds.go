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

// ConservativeRecurrenceBounds computes bounds for top-level VEVENT, VTODO and
// VJOURNAL components without under-approximating their recurrence windows.
func ConservativeRecurrenceBounds(raw string) RecurrenceBounds {
	components := recurringComponents(raw)
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
		until, known := boundedRecurrenceUntil(*component.dtstart, component.dtend, component.duration, component.allDay, component.rrule)
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
		return &until
	}
	if !c.dtend.After(*start) {
		return nil
	}
	until := *c.dtend
	return &until
}

func recurringComponents(raw string) []recurringComponent {
	parsedComponents := topLevelComponents(raw, isRecurringComponentName)
	components := make([]recurringComponent, 0, len(parsedComponents))
	for _, parsed := range parsedComponents {
		current := recurringComponent{}
		for _, line := range parsed.malformedProperties {
			if strings.EqualFold(propertyName(strings.TrimSpace(line)), "RDATE") {
				current.hasRDate = true
				current.unsafeStart = true
				current.unsafeUntil = true
			}
		}
		for _, parsedProperty := range parsed.properties {
			keyPart := strings.TrimSpace(parsedProperty.KeyPart)
			value := strings.TrimSpace(parsedProperty.Value)
			property := strings.ToUpper(propertyName(keyPart))
			switch property {
			case "DTSTART":
				if current.dtstartSeen {
					current.unsafeStart = true
					current.unsafeUntil = true
					continue
				}
				current.dtstartSeen = true
				if parsed, ok := parsePropertyDateTime(keyPart, value); ok {
					current.dtstart = timePointer(parsed)
					current.allDay = len(value) == len("20060102") || PropertyParamEquals(keyPart, "VALUE", "DATE")
				}
			case "DTEND":
				if current.dtendSeen {
					current.unsafeUntil = true
					continue
				}
				current.dtendSeen = true
				if parsed, ok := parsePropertyDateTime(keyPart, value); ok {
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
				if parsed, ok := parsePropertyDateTime(keyPart, value); ok {
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
				starts, ok := rdateStarts(keyPart, value)
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

func rdateStarts(keyPart, value string) ([]time.Time, bool) {
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
		if parsed, parsedOK := parsePropertyDateTime(keyPart, part); parsedOK {
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

func boundedRecurrenceUntil(dtstart time.Time, dtend *time.Time, durationProperty *time.Duration, allDay bool, rrule string) (*time.Time, bool) {
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
		} else if allDay {
			duration = 24 * time.Hour
		}
	}

	rule, ok := parseRecurrenceRule(rrule, dtstart.Location())
	if !ok {
		return nil, false
	}
	if rule.Until != nil {
		until := rule.Until.Add(duration)
		return &until, true
	}
	if rule.Count == 0 || hasRRuleByPart(rrule) {
		return nil, false
	}

	steps := (rule.Count - 1) * rule.Interval
	var lastStart time.Time
	switch rule.Freq {
	case "DAILY":
		lastStart = dtstart.AddDate(0, 0, steps)
	case "WEEKLY":
		lastStart = dtstart.AddDate(0, 0, 7*steps)
	default:
		return nil, false
	}
	until := lastStart.Add(duration + recurrenceBoundPad)
	return &until, true
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
