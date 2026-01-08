package dav

import (
	"strings"
	"time"
)

func extractICalDateTimes(ical string) []time.Time {
	lines := unfoldICalLines(ical)
	var times []time.Time
	for _, line := range lines {
		if line == "" {
			continue
		}
		upper := strings.ToUpper(line)
		if !strings.HasPrefix(upper, "DTSTART") && !strings.HasPrefix(upper, "DTEND") {
			continue
		}
		colonIdx := strings.Index(line, ":")
		if colonIdx == -1 {
			continue
		}
		propPart := line[:colonIdx]
		value := strings.TrimSpace(line[colonIdx+1:])
		if value == "" {
			continue
		}
		var tzid string
		if semiIdx := strings.Index(propPart, ";"); semiIdx != -1 {
			params := strings.Split(propPart[semiIdx+1:], ";")
			for _, param := range params {
				if strings.HasPrefix(strings.ToUpper(param), "TZID=") {
					tzid = strings.TrimSpace(param[len("TZID="):])
					break
				}
			}
		}
		var parsed time.Time
		var err error
		if tzid != "" {
			if loc, locErr := time.LoadLocation(tzid); locErr == nil {
				parsed, err = parseICalDateTimeInLocation(value, loc)
			} else {
				parsed, err = parseICalDateTime(value)
			}
		} else {
			parsed, err = parseICalDateTime(value)
		}
		if err == nil {
			times = append(times, parsed)
		}
	}
	return times
}

func unfoldICalLines(ical string) []string {
	ical = strings.ReplaceAll(ical, "\r\n", "\n")
	ical = strings.ReplaceAll(ical, "\r", "\n")
	rawLines := strings.Split(ical, "\n")
	var lines []string
	for _, line := range rawLines {
		if len(lines) > 0 && (strings.HasPrefix(line, " ") || strings.HasPrefix(line, "\t")) {
			lines[len(lines)-1] += strings.TrimLeft(line, " \t")
			continue
		}
		lines = append(lines, line)
	}
	return lines
}
