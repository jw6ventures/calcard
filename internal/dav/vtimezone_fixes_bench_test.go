package dav

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/store"
)

// BenchmarkValidateObjectNamingAHostZone measures a write of the common shape
// a script or a lightweight client sends, a weekly event in an IANA zone it
// does not define, against the same event stored with its definition, which is
// what a client reading it back writes.
func BenchmarkValidateObjectNamingAHostZone(b *testing.B) {
	raw := producerObject("VEVENT", "UID:bench", "DTSTAMP:20261002T120000Z", berlinEvent+"RRULE:FREQ=WEEKLY;BYDAY=MO")
	validated, fault := validateCalendarObjectForStorage(raw, nil)
	if fault != nil {
		b.Fatalf("refused: %+v", fault)
	}
	for name, body := range map[string]string{"undefined": raw, "defined": validated.Body} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, fault := validateCalendarObjectForStorage(body, nil); fault != nil {
					b.Fatalf("refused: %+v", fault)
				}
			}
		})
	}
}

// BenchmarkValidateRefusedObjectNamingAHostZone measures the refusal of the
// same event by a collection that holds tasks only, which the definition the
// validator would store for an accepted object has no part in.
func BenchmarkValidateRefusedObjectNamingAHostZone(b *testing.B) {
	raw := producerObject("VEVENT", "UID:bench", "DTSTAMP:20261002T120000Z", berlinEvent+"RRULE:FREQ=WEEKLY;BYDAY=MO")
	tasks := &store.CalendarAccess{Calendar: store.Calendar{SupportedComponents: []string{"VTODO"}}}
	if _, fault := validateCalendarObjectForStorage(raw, tasks); fault == nil {
		b.Fatal("accepted an event into a task collection")
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, fault := validateCalendarObjectForStorage(raw, tasks); fault == nil {
			b.Fatal("accepted an event into a task collection")
		}
	}
}

// BenchmarkValidateOutlookExport measures a write of an event in a zone only
// its own VTIMEZONE defines, such as Outlook's "Eastern Standard Time", whose
// name the host's time zone database cannot load.
func BenchmarkValidateOutlookExport(b *testing.B) {
	b.ReportAllocs()
	for b.Loop() {
		if _, fault := validateCalendarObjectForStorage(outlookExport, nil); fault != nil {
			b.Fatalf("refused: %+v", fault)
		}
	}
}

// foldedDescriptionObject is an event in a zone the host defines whose
// DESCRIPTION is folded after every octet, the most continuation lines a body
// of size octets can hold.
func foldedDescriptionObject(size int) string {
	const head = "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Producer//EN\r\nBEGIN:VEVENT\r\nUID:folded\r\n" +
		"DTSTAMP:20261002T120000Z\r\n" + berlinEvent + "DESCRIPTION:x"
	const tail = "\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"
	const fold = "\r\n x"
	folds := max(0, (size-len(head)-len(tail))/len(fold))
	return head + strings.Repeat(fold, folds) + tail
}

// BenchmarkValidateObjectWithAHeavilyFoldedLine measures the validator on an
// object naming a host zone whose one long line is folded after every octet,
// at two sizes ten times apart.
func BenchmarkValidateObjectWithAHeavilyFoldedLine(b *testing.B) {
	for _, size := range []int{64 << 10, 640 << 10} {
		raw := foldedDescriptionObject(size)
		if _, fault := validateCalendarObjectForStorage(raw, nil); fault != nil {
			b.Fatalf("refused: %+v", fault)
		}
		b.Run(fmt.Sprintf("%dKiB", size>>10), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, fault := validateCalendarObjectForStorage(raw, nil); fault != nil {
					b.Fatalf("refused: %+v", fault)
				}
			}
		})
	}
}

// manyDenseZonesObject is an event naming m zones the object defines itself,
// each by two monthly observance rules from Exchange's 1601 DTSTART, every
// zone's rules distinct, with eight values in each zone spread over a century.
func manyDenseZonesObject(m int) string {
	var b strings.Builder
	b.WriteString("BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Bench//EN\r\n")
	for i := range m {
		start := fmt.Sprintf("16010101T%02d%02d%02d", 2, i/60%60, i%60)
		fmt.Fprintf(&b, "BEGIN:VTIMEZONE\r\nTZID:Dense/Zone%d\r\n"+
			"BEGIN:STANDARD\r\nDTSTART:%s\r\nTZOFFSETFROM:+0100\r\nTZOFFSETTO:+0000\r\nRRULE:FREQ=MONTHLY;BYDAY=1SU\r\nEND:STANDARD\r\n"+
			"BEGIN:DAYLIGHT\r\nDTSTART:%s\r\nTZOFFSETFROM:+0000\r\nTZOFFSETTO:+0100\r\nRRULE:FREQ=MONTHLY;BYDAY=3SU\r\nEND:DAYLIGHT\r\n"+
			"END:VTIMEZONE\r\n", i, start, start)
	}
	b.WriteString("BEGIN:VEVENT\r\nUID:dense\r\nDTSTAMP:20260101T000000Z\r\nDTSTART;TZID=Dense/Zone0:20260105T090000\r\nRRULE:FREQ=WEEKLY;COUNT=10\r\n")
	for i := range m {
		var values []string
		for year := 1950; year < 2050; year += 13 {
			values = append(values, fmt.Sprintf("%d0610T090000", year))
		}
		fmt.Fprintf(&b, "RDATE;TZID=Dense/Zone%d:%s\r\n", i, strings.Join(values, ","))
	}
	b.WriteString("END:VEVENT\r\nEND:VCALENDAR\r\n")
	return b.String()
}

// BenchmarkValidateObjectWithManyDenseZones measures a write of an object
// whose zones' rules are too dense to list from their 1601 DTSTART to the
// present, and too many for the onset cache to hold together.
func BenchmarkValidateObjectWithManyDenseZones(b *testing.B) {
	for _, m := range []int{250, 500} {
		raw := manyDenseZonesObject(m)
		b.Run(fmt.Sprintf("M=%d", m), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, fault := validateCalendarObjectForStorage(raw, nil); fault != nil {
					b.Fatalf("refused: %+v", fault)
				}
			}
		})
	}
}

// BenchmarkTimezoneOnsetsDenseRule resolves a thousand instants half a year
// apart through a monthly rule from 1601, in a fresh cache each time, with and
// without COUNT.
func BenchmarkTimezoneOnsetsDenseRule(b *testing.B) {
	dtstart := time.Date(1601, 1, 1, 2, 0, 0, 0, time.UTC)
	start := time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
	for name, rrule := range map[string]string{"uncounted": "FREQ=MONTHLY;BYDAY=1SU", "counted": "FREQ=MONTHLY;BYDAY=1SU;COUNT=100000"} {
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				onsets := newTimezoneOnsets(maxHeldOnsets)
				for i := range 1000 {
					onsets.LatestOnOrBefore(dtstart, start.AddDate(0, 6*i, 0), rrule)
				}
			}
		})
	}
}

// incompleteLargeObject is a one-day event of about size octets, its long
// DESCRIPTION folded every 74 octets, that a write completes three ways: its
// DATE DTEND on its DTSTART moved to the next day, VERSION, PRODID and DTSTAMP
// added, and the host's definition of the zone it names stored with it.
func incompleteLargeObject(size int) string {
	var b strings.Builder
	b.WriteString("BEGIN:VCALENDAR\r\nBEGIN:VEVENT\r\nUID:large\r\nDTSTART;VALUE=DATE:20260301\r\nDTEND;VALUE=DATE:20260301\r\n" +
		"X-REMINDER;TZID=Europe/Berlin:20260301T090000\r\nDESCRIPTION:")
	for b.Len() < size {
		b.WriteString(strings.Repeat("x", 73) + "\r\n ")
	}
	b.WriteString("x\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n")
	return b.String()
}

// BenchmarkValidateObjectNeedingEveryCompletion measures a write of large
// objects every completion applies to.
func BenchmarkValidateObjectNeedingEveryCompletion(b *testing.B) {
	for _, size := range []int{64 << 10, 1 << 20} {
		raw := incompleteLargeObject(size)
		b.Run(fmt.Sprintf("%dKiB", size>>10), func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				if _, fault := validateCalendarObjectForStorage(raw, nil); fault != nil {
					b.Fatalf("refused: %+v", fault)
				}
			}
		})
	}
}
