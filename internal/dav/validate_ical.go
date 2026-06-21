package dav

import (
	"fmt"
	"strconv"
	"strings"
)

// validateICalendar performs basic validation of iCalendar data (RFC 5545)
func (h *DavServer) validateICalendar(data string) error {
	trimmed := strings.TrimSpace(data)

	if !strings.HasPrefix(strings.ToUpper(trimmed), "BEGIN:VCALENDAR") {
		return fmt.Errorf("missing BEGIN:VCALENDAR")
	}

	// Must end with END:VCALENDAR
	if !strings.HasSuffix(strings.ToUpper(trimmed), "END:VCALENDAR") {
		return fmt.Errorf("missing END:VCALENDAR")
	}

	componentTypes := extractICalComponentTypes(data)
	_, hasEvent := componentTypes["VEVENT"]
	_, hasTodo := componentTypes["VTODO"]
	_, hasJournal := componentTypes["VJOURNAL"]
	_, hasFreeBusy := componentTypes["VFREEBUSY"]
	hasComponent := hasEvent || hasTodo || hasJournal || hasFreeBusy

	if !hasComponent {
		return fmt.Errorf("no calendar component found (VEVENT, VTODO, VJOURNAL, or VFREEBUSY required)")
	}

	upper := strings.ToUpper(trimmed)
	if err := validateBalancedTags(upper); err != nil {
		return err
	}

	return nil
}

// validateBalancedTags checks that all BEGIN tags have matching END tags
func validateBalancedTags(data string) error {
	lines := strings.Split(data, "\n")
	var stack []string

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "BEGIN:") {
			componentType := strings.TrimPrefix(line, "BEGIN:")
			stack = append(stack, componentType)
		} else if strings.HasPrefix(line, "END:") {
			componentType := strings.TrimPrefix(line, "END:")
			if len(stack) == 0 {
				return fmt.Errorf("END:%s without matching BEGIN", componentType)
			}
			if stack[len(stack)-1] != componentType {
				return fmt.Errorf("mismatched tags: BEGIN:%s ... END:%s", stack[len(stack)-1], componentType)
			}
			stack = stack[:len(stack)-1]
		}
	}

	if len(stack) > 0 {
		return fmt.Errorf("unbalanced tags: BEGIN:%s without matching END", stack[len(stack)-1])
	}

	return nil
}

func extractICalComponentTypes(icalData string) map[string]struct{} {
	types := make(map[string]struct{})
	lines := unfoldICalLines(icalData)
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		upper := strings.ToUpper(line)
		if strings.HasPrefix(upper, "BEGIN:") {
			componentType := strings.TrimSpace(strings.TrimPrefix(upper, "BEGIN:"))
			if componentType != "" {
				types[componentType] = struct{}{}
			}
		}
	}
	return types
}

func extractICalRRULECount(icalData string) (int, bool) {
	lines := unfoldICalLines(icalData)
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		upper := strings.ToUpper(line)
		if strings.HasPrefix(upper, "RRULE") {
			colonIdx := strings.Index(line, ":")
			if colonIdx == -1 {
				continue
			}
			rule := line[colonIdx+1:]
			parts := strings.Split(rule, ";")
			for _, part := range parts {
				part = strings.TrimSpace(part)
				if part == "" {
					continue
				}
				if idx := strings.Index(part, "="); idx != -1 {
					if strings.EqualFold(part[:idx], "COUNT") {
						value := strings.TrimSpace(part[idx+1:])
						if value == "" {
							continue
						}
						if count, err := strconv.Atoi(value); err == nil {
							return count, true
						}
					}
				}
			}
		}
	}
	return 0, false
}

func countICalAttendees(icalData string) int {
	lines := unfoldICalLines(icalData)
	targets := map[string]struct{}{
		"VEVENT":   {},
		"VTODO":    {},
		"VJOURNAL": {},
	}
	inTarget := false
	currentCount := 0
	maxCount := 0

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		upper := strings.ToUpper(line)
		if strings.HasPrefix(upper, "BEGIN:") {
			name := strings.TrimSpace(strings.TrimPrefix(upper, "BEGIN:"))
			if !inTarget {
				if _, ok := targets[name]; ok {
					inTarget = true
					currentCount = 0
				}
			}
			continue
		}
		if strings.HasPrefix(upper, "END:") {
			name := strings.TrimSpace(strings.TrimPrefix(upper, "END:"))
			if inTarget {
				if _, ok := targets[name]; ok {
					if currentCount > maxCount {
						maxCount = currentCount
					}
					inTarget = false
				}
			}
			continue
		}
		if !inTarget {
			continue
		}
		if strings.HasPrefix(upper, "ATTENDEE") {
			if len(upper) == len("ATTENDEE") || (len(upper) > len("ATTENDEE") && (upper[len("ATTENDEE")] == ';' || upper[len("ATTENDEE")] == ':')) {
				currentCount++
			}
		}
	}
	if currentCount > maxCount {
		maxCount = currentCount
	}
	return maxCount
}

// extractUIDFromICalendar extracts the UID property from iCalendar data.
// For multi-component calendars, returns the UID from the first top-level component.
// The validateCalendarObjectResource function handles validation of multi-component UIDs.
func extractUIDFromICalendar(icalData string) (string, error) {
	components := parseCalendarTopLevelComponents(icalData)
	if len(components) == 0 {
		return "", fmt.Errorf("no calendar components found")
	}
	// Return UID from first top-level component
	firstComponent := components[0]
	if firstComponent.UIDEmpty || firstComponent.UIDCount == 0 {
		return "", fmt.Errorf("no UID property found in calendar data")
	}
	if firstComponent.UID == "" {
		return "", fmt.Errorf("empty UID property")
	}
	return firstComponent.UID, nil
}

type calendarTopLevelComponent struct {
	Type            string
	UID             string
	UIDCount        int
	UIDEmpty        bool
	HasRecurrenceID bool
}

func validateCalendarObjectResource(icalData string) []string {
	components := parseCalendarTopLevelComponents(icalData)
	for _, component := range components {
		if component.UIDEmpty || component.UIDCount == 0 {
			return []string{"valid-calendar-object-resource"}
		}
		if component.UIDCount > 1 {
			return []string{"valid-calendar-data"}
		}
	}
	if len(components) <= 1 {
		return nil
	}

	uid := components[0].UID
	sameUID := true
	for _, component := range components[1:] {
		if component.UID != uid {
			sameUID = false
			break
		}
	}
	if !sameUID {
		return []string{"valid-calendar-object-resource", "valid-calendar-data"}
	}

	withoutRecurrence := 0
	withRecurrence := 0
	for _, component := range components {
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

func parseCalendarTopLevelComponents(icalData string) []calendarTopLevelComponent {
	lines := unfoldICalLines(icalData)
	var stack []string
	var current *calendarTopLevelComponent
	var components []calendarTopLevelComponent

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		upper := strings.ToUpper(line)
		if strings.HasPrefix(upper, "BEGIN:") {
			componentType := strings.TrimSpace(strings.TrimPrefix(upper, "BEGIN:"))
			stack = append(stack, componentType)
			if len(stack) == 2 && stack[0] == "VCALENDAR" && isTopLevelComponentType(componentType) {
				current = &calendarTopLevelComponent{Type: componentType}
			}
			continue
		}
		if strings.HasPrefix(upper, "END:") {
			componentType := strings.TrimSpace(strings.TrimPrefix(upper, "END:"))
			if current != nil && len(stack) == 2 && stack[0] == "VCALENDAR" && stack[1] == current.Type && componentType == current.Type {
				components = append(components, *current)
				current = nil
			}
			if len(stack) > 0 {
				stack = stack[:len(stack)-1]
			}
			continue
		}
		if current == nil {
			continue
		}
		if strings.HasPrefix(upper, "UID") {
			if len(upper) == len("UID") || (len(upper) > len("UID") && (upper[len("UID")] == ':' || upper[len("UID")] == ';')) {
				colonIdx := strings.Index(line, ":")
				if colonIdx == -1 {
					continue
				}
				uid := strings.TrimSpace(line[colonIdx+1:])
				current.UIDCount++
				if uid == "" {
					current.UIDEmpty = true
					continue
				}
				if current.UID == "" {
					current.UID = uid
				}
			}
			continue
		}
		if strings.HasPrefix(upper, "RECURRENCE-ID") {
			current.HasRecurrenceID = true
		}
	}

	if current != nil {
		components = append(components, *current)
	}

	return components
}

func isTopLevelComponentType(componentType string) bool {
	switch componentType {
	case "VEVENT", "VTODO", "VJOURNAL", "VFREEBUSY":
		return true
	default:
		return false
	}
}

func containsICalMethodProperty(icalData string) bool {
	lines := unfoldICalLines(icalData)
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		upper := strings.ToUpper(line)
		if strings.HasPrefix(upper, "METHOD") {
			if len(upper) == len("METHOD") || (len(upper) > len("METHOD") && (upper[len("METHOD")] == ':' || upper[len("METHOD")] == ';')) {
				return true
			}
		}
	}
	return false
}

func hasMultipleDifferentUIDs(icalData string) bool {
	components := parseCalendarTopLevelComponents(icalData)
	if len(components) <= 1 {
		return false
	}
	first := components[0].UID
	for _, component := range components[1:] {
		if component.UID != first {
			return true
		}
	}
	return false
}
