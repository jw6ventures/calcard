package dav

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jw6ventures/calcard/internal/ical"
	"github.com/jw6ventures/calcard/internal/store"
)

type calendarAnalysis struct {
	ComponentTypes map[string]struct{}
	Components     []calendarTopLevelComponent
	HasMethod      bool
	DateTimes      []time.Time
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
	Metadata        store.EventWriteMetadata
}

func analyzeICalendar(raw string) (calendarAnalysis, error) {
	analysis := calendarAnalysis{ComponentTypes: make(map[string]struct{})}
	trimmed := strings.TrimSpace(raw)
	upperTrimmed := strings.ToUpper(trimmed)
	if !strings.HasPrefix(upperTrimmed, "BEGIN:VCALENDAR") {
		return analysis, fmt.Errorf("missing BEGIN:VCALENDAR")
	}
	if !strings.HasSuffix(upperTrimmed, "END:VCALENDAR") {
		return analysis, fmt.Errorf("missing END:VCALENDAR")
	}

	lines := ical.UnfoldLines(raw)
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
		switch name {
		case "METHOD":
			analysis.HasMethod = true
		case "DTSTART", "DTEND":
			if parsed, ok := ical.ParsePropertyDateTimeLocal(keyPart, value); ok {
				analysis.DateTimes = append(analysis.DateTimes, parsed)
			}
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
			case "ATTENDEE":
				if attendeeLimitedComponent(current.Type) {
					currentAttendees++
				}
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
				applyEventMetadataProperty(&current.Metadata, name, keyPart, value)
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
	applyRecurrenceMetadata(&analysis.Metadata, lines)
	return analysis, nil
}

func attendeeLimitedComponent(componentType string) bool {
	return componentType == "VEVENT" || componentType == "VTODO" || componentType == "VJOURNAL"
}

func recurrenceLimitedComponent(componentType string) bool {
	return componentType == "VEVENT" || componentType == "VTODO" || componentType == "VJOURNAL"
}

func primaryEventMetadata(components []calendarTopLevelComponent) store.EventWriteMetadata {
	var fallback *store.EventWriteMetadata
	for i := range components {
		component := &components[i]
		if component.Type != "VEVENT" {
			continue
		}
		if fallback == nil {
			fallback = &component.Metadata
		}
		if !component.HasRecurrenceID {
			return component.Metadata
		}
	}
	if fallback != nil {
		return *fallback
	}
	return store.EventWriteMetadata{}
}

func splitICalendarProperty(line string) (name, keyPart, value string, ok bool) {
	colon := strings.IndexByte(line, ':')
	if colon < 0 {
		return "", "", "", false
	}
	keyPart = line[:colon]
	value = line[colon+1:]
	name = keyPart
	if semicolon := strings.IndexByte(name, ';'); semicolon >= 0 {
		name = name[:semicolon]
	}
	name = strings.ToUpper(strings.TrimSpace(name))
	return name, keyPart, value, name != ""
}

func rruleCount(rule string) (int, bool) {
	for _, part := range strings.Split(rule, ";") {
		key, value, ok := strings.Cut(part, "=")
		if !ok || !strings.EqualFold(strings.TrimSpace(key), "COUNT") {
			continue
		}
		count, err := strconv.Atoi(strings.TrimSpace(value))
		return count, err == nil
	}
	return 0, false
}

func applyEventMetadataProperty(metadata *store.EventWriteMetadata, name, keyPart, value string) {
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
			parsed, ok := ical.ParsePropertyDateTimeLocal(keyPart, value)
			if !ok {
				return
			}
			metadata.DTStart = &parsed
			metadata.AllDay = len(strings.TrimSpace(value)) == len("20060102") || ical.PropertyParamEquals(keyPart, "VALUE", "DATE")
		}
	case "DTEND":
		if metadata.DTEnd == nil {
			parsed, ok := ical.ParsePropertyDateTimeLocal(keyPart, value)
			if !ok {
				return
			}
			metadata.DTEnd = &parsed
		}
	}
}

func applyRecurrenceMetadata(metadata *store.EventWriteMetadata, lines []string) {
	bounds := ical.ConservativeRecurrenceBoundsFromUnfoldedLines(lines)
	if !bounds.Recurring {
		return
	}
	metadata.RecurrenceStart = bounds.Start
	metadata.RecurrenceUntil = bounds.Until
	if bounds.StartUnknown {
		value := time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
		metadata.RecurrenceStart = &value
	}
	if bounds.UntilUnknown {
		value := time.Date(9999, 12, 31, 23, 59, 59, 0, time.UTC)
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

func (a calendarAnalysis) objectValidationConditions() []string {
	for _, component := range a.Components {
		if component.UIDEmpty || component.UIDCount == 0 {
			return []string{"valid-calendar-object-resource"}
		}
		if component.UIDCount > 1 {
			return []string{"valid-calendar-data"}
		}
	}
	if len(a.Components) <= 1 {
		return nil
	}
	uid := a.Components[0].UID
	withoutRecurrence := 0
	withRecurrence := 0
	for _, component := range a.Components {
		if component.UID != uid {
			return []string{"valid-calendar-object-resource", "valid-calendar-data"}
		}
		if component.HasRecurrenceID {
			withRecurrence++
		} else {
			withoutRecurrence++
		}
	}
	if withRecurrence > 0 && withoutRecurrence == 1 {
		return nil
	}
	return []string{"valid-calendar-object-resource"}
}

func (a calendarAnalysis) uid() (string, error) {
	if len(a.Components) == 0 || a.Components[0].UIDEmpty || a.Components[0].UIDCount == 0 || a.Components[0].UID == "" {
		return "", fmt.Errorf("no UID property found in calendar data")
	}
	return a.Components[0].UID, nil
}
