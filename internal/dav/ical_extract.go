package dav

import (
	"strings"
	"time"

	"github.com/jw6ventures/calcard/internal/ical"
)

func extractICalDateTimes(raw string) []time.Time {
	lines := ical.UnfoldLines(raw)
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
				parsed, err = ical.ParseDateTimeInLocation(value, loc)
			} else {
				parsed, err = ical.ParseDateTime(value)
			}
		} else {
			parsed, err = ical.ParseDateTime(value)
		}
		if err == nil {
			times = append(times, parsed)
		}
	}
	return times
}
