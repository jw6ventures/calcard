package dav

import "strings"

func reportCalendarData(report reportRequest) *calendarDataEl {
	if report.Prop != nil && report.Prop.CalendarData != nil {
		return report.Prop.CalendarData
	}
	if report.CalendarData != nil {
		return report.CalendarData
	}
	return nil
}

func filterICalendarData(raw string, calData *calendarDataEl) string {
	if calData == nil {
		return raw
	}
	if len(calData.Comp) == 0 && len(calData.Prop) == 0 {
		return raw
	}
	allowAllComponents := len(calData.Comp) == 0 && len(calData.Prop) > 0
	globalProps := calData.Prop
	calData = normalizeCalendarData(calData)

	normalized := strings.ReplaceAll(raw, "\r\n", "\n")
	normalized = strings.ReplaceAll(normalized, "\r", "\n")
	lines := strings.Split(normalized, "\n")

	var out []string
	var compStack []string
	var allowStack []*calendarComp
	var keepStack []bool
	var lastIncluded bool

	for _, line := range lines {
		line = strings.TrimRight(line, "\r")
		upper := strings.ToUpper(line)
		if strings.HasPrefix(upper, "BEGIN:") {
			name := strings.ToUpper(strings.TrimSpace(line[len("BEGIN:"):]))
			parentKeep := len(keepStack) == 0 || keepStack[len(keepStack)-1]
			var match *calendarComp
			if parentKeep {
				if len(allowStack) == 0 || allowStack[len(allowStack)-1] == nil {
					if allowAllComponents {
						match = &calendarComp{Name: name, Prop: globalProps}
					} else {
						for i := range calData.Comp {
							if strings.EqualFold(calData.Comp[i].Name, name) {
								match = &calData.Comp[i]
								break
							}
						}
					}
				} else {
					parent := allowStack[len(allowStack)-1]
					if len(parent.Comp) == 0 {
						if allowAllComponents {
							match = &calendarComp{Name: name, Prop: globalProps}
						} else {
							match = &calendarComp{Name: name}
						}
					} else {
						for i := range parent.Comp {
							if strings.EqualFold(parent.Comp[i].Name, name) {
								match = &parent.Comp[i]
								break
							}
						}
					}
				}
			}
			keep := parentKeep && match != nil
			compStack = append(compStack, name)
			allowStack = append(allowStack, match)
			keepStack = append(keepStack, keep)
			if keep {
				out = append(out, line)
			}
			lastIncluded = false
			continue
		}

		if strings.HasPrefix(upper, "END:") {
			if len(compStack) == 0 {
				continue
			}
			if keepStack[len(keepStack)-1] {
				out = append(out, line)
			}
			compStack = compStack[:len(compStack)-1]
			allowStack = allowStack[:len(allowStack)-1]
			keepStack = keepStack[:len(keepStack)-1]
			lastIncluded = false
			continue
		}

		if len(compStack) == 0 {
			lastIncluded = false
			continue
		}

		if strings.HasPrefix(line, " ") || strings.HasPrefix(line, "\t") {
			if keepStack[len(keepStack)-1] && lastIncluded {
				out = append(out, line)
			}
			continue
		}

		if !keepStack[len(keepStack)-1] {
			lastIncluded = false
			continue
		}

		allowed := true
		current := allowStack[len(allowStack)-1]
		if current != nil && len(current.Prop) > 0 {
			allowed = false
			propName := strings.ToUpper(line)
			if idx := strings.IndexAny(propName, ":;"); idx != -1 {
				propName = propName[:idx]
			}
			for i := range current.Prop {
				if strings.EqualFold(current.Prop[i].Name, propName) {
					allowed = true
					break
				}
			}
		}

		if allowed {
			out = append(out, line)
			lastIncluded = true
		} else {
			lastIncluded = false
		}
	}

	if len(out) == 0 {
		return ""
	}
	filtered := strings.Join(out, "\r\n") + "\r\n"
	if strings.Contains(strings.ToUpper(filtered), "BEGIN:VCALENDAR") {
		return filtered
	}
	header := extractVCalendarHeader(lines)
	trimmed := strings.TrimSuffix(filtered, "\r\n")
	filterLines := strings.Split(trimmed, "\r\n")
	var wrapped []string
	wrapped = append(wrapped, "BEGIN:VCALENDAR")
	wrapped = append(wrapped, header...)
	wrapped = append(wrapped, filterLines...)
	wrapped = append(wrapped, "END:VCALENDAR")
	return strings.Join(wrapped, "\r\n") + "\r\n"
}

func normalizeCalendarData(calData *calendarDataEl) *calendarDataEl {
	hasVCalendar := false
	for i := range calData.Comp {
		if strings.EqualFold(calData.Comp[i].Name, "VCALENDAR") {
			hasVCalendar = true
			break
		}
	}
	if hasVCalendar {
		return calData
	}
	wrapped := calendarComp{Name: "VCALENDAR", Comp: calData.Comp, Prop: calData.Prop}
	return &calendarDataEl{Comp: []calendarComp{wrapped}}
}

func extractVCalendarHeader(lines []string) []string {
	var header []string
	inCalendar := false
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if !inCalendar {
			if strings.EqualFold(trimmed, "BEGIN:VCALENDAR") {
				inCalendar = true
			}
			continue
		}
		if strings.HasPrefix(strings.ToUpper(trimmed), "END:VCALENDAR") {
			break
		}
		if strings.HasPrefix(strings.ToUpper(trimmed), "BEGIN:") {
			break
		}
		if trimmed == "" {
			continue
		}
		header = append(header, line)
	}
	return header
}
