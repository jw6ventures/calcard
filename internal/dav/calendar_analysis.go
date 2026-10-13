package dav

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/jw6ventures/calcard/internal/ical"
	"github.com/jw6ventures/calcard/internal/store"
)

// calendarAnalysis is the reading of a calendar object the handler needs once
// validateCalendarObject has accepted it: the components to check against the
// collection's own restrictions, the recurrence and attendee counts the §5.2
// limits bound, and the metadata the store indexes.
type calendarAnalysis struct {
	ComponentTypes map[string]struct{}
	Components     []calendarTopLevelComponent
	MaxAttendees   int
	MaxRRULECount  int
	HasRRULECount  bool
	Metadata       store.EventWriteMetadata
}

type calendarTopLevelComponent struct {
	Type            string
	UID             string
	UIDCount        int
	UIDEmpty        bool
	HasRecurrenceID bool
	HasDuration     bool
	Metadata        store.EventWriteMetadata
}

func analyzeICalendar(raw string) (calendarAnalysis, error) {
	lines := ical.UnfoldLines(raw)
	return analyzeUnfoldedICalendar(raw, lines, ical.NewObjectTimesSharingOnsets(lines, sharedTimezoneOnsets))
}

// analyzeUnfoldedICalendar is analyzeICalendar for a caller that already
// unfolded raw into lines, reading its dates through times, the ObjectTimes of
// those lines.
func analyzeUnfoldedICalendar(raw string, lines []string, times *ical.ObjectTimes) (calendarAnalysis, error) {
	analysis := calendarAnalysis{ComponentTypes: make(map[string]struct{})}
	trimmed := strings.TrimSpace(raw)
	upperTrimmed := strings.ToUpper(trimmed)
	if !strings.HasPrefix(upperTrimmed, "BEGIN:VCALENDAR") {
		return analysis, fmt.Errorf("missing BEGIN:VCALENDAR")
	}
	if !strings.HasSuffix(upperTrimmed, "END:VCALENDAR") {
		return analysis, fmt.Errorf("missing END:VCALENDAR")
	}

	var stack []string
	var current *calendarTopLevelComponent
	currentAttendees := 0
	rootClosed := false
	for _, rawLine := range lines {
		line := strings.TrimSpace(rawLine)
		if line == "" {
			continue
		}
		upper := strings.ToUpper(line)
		if strings.HasPrefix(upper, "BEGIN:") {
			componentType := strings.TrimSpace(strings.TrimPrefix(upper, "BEGIN:"))
			if componentType == "" {
				return analysis, fmt.Errorf("empty component name")
			}
			if len(stack) == 0 {
				if rootClosed || componentType != "VCALENDAR" {
					return analysis, fmt.Errorf("expected one VCALENDAR root")
				}
			} else if componentType == "VCALENDAR" {
				return analysis, fmt.Errorf("nested VCALENDAR component")
			}
			analysis.ComponentTypes[componentType] = struct{}{}
			stack = append(stack, componentType)
			if len(stack) == 2 && stack[0] == "VCALENDAR" && isTopLevelComponentType(componentType) {
				current = &calendarTopLevelComponent{Type: componentType}
				currentAttendees = 0
			}
			continue
		}
		if strings.HasPrefix(upper, "END:") {
			componentType := strings.TrimSpace(strings.TrimPrefix(upper, "END:"))
			if len(stack) == 0 {
				return analysis, fmt.Errorf("END:%s without matching BEGIN", componentType)
			}
			if stack[len(stack)-1] != componentType {
				return analysis, fmt.Errorf("mismatched tags: BEGIN:%s ... END:%s", stack[len(stack)-1], componentType)
			}
			if current != nil && len(stack) == 2 && stack[0] == "VCALENDAR" && stack[1] == current.Type {
				analysis.Components = append(analysis.Components, *current)
				if currentAttendees > analysis.MaxAttendees {
					analysis.MaxAttendees = currentAttendees
				}
				current = nil
			}
			if len(stack) == 1 && componentType == "VCALENDAR" {
				rootClosed = true
			}
			stack = stack[:len(stack)-1]
			continue
		}
		if len(stack) == 0 {
			return analysis, fmt.Errorf("content outside VCALENDAR root")
		}
		name, keyPart, value, ok := splitICalendarProperty(line)
		if !ok {
			continue
		}
		if current != nil && name == "ATTENDEE" && attendeeLimitedComponent(current.Type) {
			currentAttendees++
		}
		if current != nil && len(stack) == 2 {
			switch name {
			case "UID":
				current.UIDCount++
				uid := strings.TrimSpace(value)
				if uid == "" {
					current.UIDEmpty = true
				} else if current.UID == "" {
					current.UID = uid
				}
			case "RECURRENCE-ID":
				current.HasRecurrenceID = true
			case "DURATION":
				current.HasDuration = true
			case "RRULE":
				if recurrenceLimitedComponent(current.Type) {
					if count, ok := rruleCount(value); ok {
						analysis.HasRRULECount = true
						if count > analysis.MaxRRULECount {
							analysis.MaxRRULECount = count
						}
					}
				}
			}
			if current.Type == "VEVENT" {
				applyEventMetadataProperty(&current.Metadata, times, name, keyPart, value)
			}
		}
	}
	if len(stack) != 0 {
		return analysis, fmt.Errorf("unbalanced tags: BEGIN:%s without matching END", stack[len(stack)-1])
	}
	if !rootClosed {
		return analysis, fmt.Errorf("missing END:VCALENDAR")
	}
	if len(analysis.Components) == 0 {
		return analysis, fmt.Errorf("no calendar component found (VEVENT, VTODO, VJOURNAL, or VFREEBUSY required)")
	}
	analysis.Metadata = primaryEventMetadata(analysis.Components)
	applyRecurrenceMetadata(&analysis.Metadata, times)
	return analysis, nil
}

func attendeeLimitedComponent(componentType string) bool {
	return componentType == "VEVENT" || componentType == "VTODO" || componentType == "VJOURNAL" || componentType == "VFREEBUSY"
}

func recurrenceLimitedComponent(componentType string) bool {
	return componentType == "VEVENT" || componentType == "VTODO" || componentType == "VJOURNAL"
}

// primaryEventMetadata is the metadata of the VEVENT the store indexes the
// object by, its end the one ical.StoredEventEnd stores.
func primaryEventMetadata(components []calendarTopLevelComponent) store.EventWriteMetadata {
	var primary *calendarTopLevelComponent
	for i := range components {
		component := &components[i]
		if component.Type != "VEVENT" {
			continue
		}
		if primary == nil {
			primary = component
		}
		if !component.HasRecurrenceID {
			primary = component
			break
		}
	}
	if primary == nil {
		return store.EventWriteMetadata{}
	}
	metadata := primary.Metadata
	metadata.DTEnd = ical.StoredEventEnd(metadata.DTStart, metadata.AllDay, metadata.DTEnd, primary.HasDuration)
	return metadata
}

func splitICalendarProperty(line string) (name, keyPart, value string, ok bool) {
	keyPart, value, ok = ical.SplitContentLine(line)
	if !ok {
		return "", "", "", false
	}
	name = keyPart
	if semicolon := strings.IndexByte(name, ';'); semicolon >= 0 {
		name = name[:semicolon]
	}
	name = strings.ToUpper(strings.TrimSpace(name))
	return name, keyPart, value, name != ""
}

func rruleCount(rule string) (int, bool) {
	value, ok := ical.RRuleParam(rule, "COUNT")
	if !ok {
		return 0, false
	}
	count, err := strconv.Atoi(value)
	return count, err == nil
}

// applyEventMetadataProperty records one property of a VEVENT in its metadata,
// reading dates through times so the stored instants are the ones the reports
// match the event at.
func applyEventMetadataProperty(metadata *store.EventWriteMetadata, times *ical.ObjectTimes, name, keyPart, value string) {
	switch name {
	case "SUMMARY":
		if metadata.Summary == nil {
			metadata.Summary = stringPtr(unescapeCalendarText(value))
		}
	case "DESCRIPTION":
		if metadata.Description == nil {
			metadata.Description = stringPtr(unescapeCalendarText(value))
		}
	case "LOCATION":
		if metadata.Location == nil {
			metadata.Location = stringPtr(unescapeCalendarText(value))
		}
	case "DTSTART":
		if metadata.DTStart == nil {
			parsed, ok := times.ResolvePropertyTime(keyPart, value)
			if !ok {
				return
			}
			metadata.DTStart = &parsed
			metadata.AllDay = len(strings.TrimSpace(value)) == len(ical.DateLayout) || ical.PropertyParamEquals(keyPart, "VALUE", "DATE")
		}
	case "DTEND":
		if metadata.DTEnd == nil {
			parsed, ok := times.ResolvePropertyTime(keyPart, value)
			if !ok {
				return
			}
			metadata.DTEnd = &parsed
		}
	}
}

func applyRecurrenceMetadata(metadata *store.EventWriteMetadata, times *ical.ObjectTimes) {
	bounds := ical.ConservativeRecurrenceBoundsOf(times)
	if !bounds.Recurring {
		return
	}
	metadata.RecurrenceStart = bounds.Start
	metadata.RecurrenceUntil = bounds.Until
	if bounds.StartUnknown {
		value := ical.RecurrenceStartSentinel
		metadata.RecurrenceStart = &value
	}
	if bounds.UntilUnknown {
		value := ical.RecurrenceUntilSentinel
		metadata.RecurrenceUntil = &value
	}
}

func unescapeCalendarText(value string) string {
	value = strings.ReplaceAll(value, `\n`, "\n")
	value = strings.ReplaceAll(value, `\N`, "\n")
	value = strings.ReplaceAll(value, `\,`, ",")
	value = strings.ReplaceAll(value, `\;`, ";")
	return strings.ReplaceAll(value, `\\`, `\`)
}

func (a calendarAnalysis) uid() (string, error) {
	if len(a.Components) == 0 || a.Components[0].UIDEmpty || a.Components[0].UIDCount == 0 || a.Components[0].UID == "" {
		return "", fmt.Errorf("no UID property found in calendar data")
	}
	return a.Components[0].UID, nil
}
