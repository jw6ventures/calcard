package vcard

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// Date is a calendar date whose year may be omitted, as BDAY and ANNIVERSARY
// allow.
type Date struct {
	Year    int
	Month   time.Month
	Day     int
	HasYear bool
}

// leapYear validates a year-less date, so --02-29 is accepted.
const leapYear = 2000

// ParseDate reads a date-only value in any form RFC 2426 or RFC 6350 writes
// one: YYYY-MM-DD, YYYYMMDD, --MM-DD or --MMDD. Anything else, including
// date-times, reduced forms and free text, is not a Date.
func ParseDate(value string) (Date, bool) {
	var year, month, day string
	switch {
	case len(value) == 10 && value[4] == '-' && value[7] == '-':
		year, month, day = value[:4], value[5:7], value[8:]
	case len(value) == 8 && value[:2] != "--":
		year, month, day = value[:4], value[4:6], value[6:]
	case len(value) == 7 && value[:2] == "--" && value[4] == '-':
		month, day = value[2:4], value[5:]
	case len(value) == 6 && value[:2] == "--":
		month, day = value[2:4], value[4:]
	default:
		return Date{}, false
	}
	d := Date{HasYear: year != ""}
	y := leapYear
	if d.HasYear {
		n, ok := digits(year)
		if !ok {
			return Date{}, false
		}
		y, d.Year = n, n
	}
	m, okM := digits(month)
	dd, okD := digits(day)
	if !okM || !okD || m < 1 || m > 12 || dd < 1 {
		return Date{}, false
	}
	if t := time.Date(y, time.Month(m), dd, 0, 0, 0, 0, time.UTC); t.Month() != time.Month(m) {
		return Date{}, false
	}
	d.Month, d.Day = time.Month(m), dd
	return d, true
}

func digits(s string) (int, bool) {
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, false
		}
	}
	n, err := strconv.Atoi(s)
	return n, err == nil
}

// Format writes the date as the given vCard version spells it: the extended
// form in 3.0 and the basic form RFC 6350 Section 4.3.1 uses in 4.0.
func (d Date) Format(version string) string {
	basic := version == "4.0"
	switch {
	case d.HasYear && basic:
		return fmt.Sprintf("%04d%02d%02d", d.Year, int(d.Month), d.Day)
	case d.HasYear:
		return fmt.Sprintf("%04d-%02d-%02d", d.Year, int(d.Month), d.Day)
	case basic:
		return fmt.Sprintf("--%02d%02d", int(d.Month), d.Day)
	default:
		return fmt.Sprintf("--%02d-%02d", int(d.Month), d.Day)
	}
}

// ParseDateProperty reads the date a BDAY or ANNIVERSARY line names. A
// date-time names the date it falls on; a VALUE=text value names none. Apple
// clients cannot write a year-less date, so they write a stand-in year and
// name it in X-APPLE-OMIT-YEAR; that year, and year 0000, read as omitted.
func ParseDateProperty(line Line) (Date, bool) {
	for _, value := range line.ParamValues("VALUE") {
		if strings.EqualFold(value, "text") {
			return Date{}, false
		}
	}
	value := strings.TrimSpace(line.Value)
	if t := strings.IndexAny(value, "Tt"); t > 0 {
		if !isTimeOfDay(value[t+1:]) {
			return Date{}, false
		}
		value = value[:t]
	}
	date, ok := ParseDate(value)
	if !ok {
		return Date{}, false
	}
	if omitted, ok := OmittedYear(line); ok && date.HasYear && date.Year == omitted {
		date.Year, date.HasYear = 0, false
	}
	// Some clients write a year-less date as year 0000.
	if date.HasYear && date.Year == 0 {
		date.HasYear = false
	}
	return date, true
}

// OmittedYear returns the stand-in year an X-APPLE-OMIT-YEAR parameter names.
func OmittedYear(line Line) (int, bool) {
	for _, value := range line.ParamValues("X-APPLE-OMIT-YEAR") {
		if year, ok := digits(value); ok && value != "" {
			return year, true
		}
	}
	return 0, false
}

// isTimeOfDay reports whether s is shaped like the time part of an ISO 8601
// date-time: hours first, then only digits, colons, fractions and a zone.
func isTimeOfDay(s string) bool {
	if len(s) < 2 || s[0] < '0' || s[0] > '9' || s[1] < '0' || s[1] > '9' {
		return false
	}
	return strings.Trim(s, "0123456789:.,Zz+-") == ""
}
