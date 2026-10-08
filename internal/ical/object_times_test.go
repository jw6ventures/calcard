package ical

import (
	"strings"
	"testing"
	"time"
)

// objectWithTimezone is a calendar object holding one VTIMEZONE, built from
// definition lines, ahead of an event.
func objectWithTimezone(definition ...string) []string {
	return UnfoldLines("BEGIN:VCALENDAR\r\nVERSION:2.0\r\n" + strings.Join(definition, "\r\n") + "\r\n" +
		"BEGIN:VEVENT\r\nUID:x\r\nDTSTART:20240304T090000Z\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n")
}

// The instants a writer stores for an object's dates are the ones the DAV
// layer's matcher reads them as: a TZID through the VTIMEZONE the object ships
// for it, even where the host's zone of that name says otherwise, unless that
// definition is one the server generated for a zone the host defines; else
// through the host's zone; else, like a floating value, as UTC.
func TestObjectTimesResolveATZIDAsTheMatcherDoes(t *testing.T) {
	requireHostZone(t, "Europe/Berlin")
	fixedPlusThree := func(extra ...string) []string {
		lines := append([]string{"BEGIN:VTIMEZONE", "TZID:Europe/Berlin"}, extra...)
		return objectWithTimezone(append(lines,
			"BEGIN:STANDARD", "DTSTART:19700101T000000", "TZOFFSETFROM:+0300", "TZOFFSETTO:+0300", "END:STANDARD",
			"END:VTIMEZONE")...)
	}
	tests := []struct {
		name                 string
		lines                []string
		keyPart, value, want string
	}{
		{"object's definition of a name the host lacks", outlookEasternObject,
			`DTSTART;TZID="Eastern Standard Time"`, "20240304T090000", "20240304T140000Z"},
		{"object's definition, in its daylight time", outlookEasternObject,
			`DTEND;TZID="Eastern Standard Time"`, "20240311T090000", "20240311T130000Z"},
		{"client definition disagreeing with the host", fixedPlusThree(),
			"DTSTART;TZID=Europe/Berlin", "20240304T090000", "20240304T060000Z"},
		{"server-generated definition of a host zone", fixedPlusThree(HostDefinitionProperty + ":TRUE"),
			"DTSTART;TZID=Europe/Berlin", "20240304T090000", "20240304T080000Z"},
		{"host zone the object does not define", outlookEasternObject,
			"DTSTART;TZID=Europe/Berlin", "20240304T090000", "20240304T080000Z"},
		{"zone neither defines", outlookEasternObject,
			"DTSTART;TZID=Nowhere/Zone", "20240304T090000", "20240304T090000Z"},
		{"UTC value beside a TZID", outlookEasternObject,
			`DTSTART;TZID="Eastern Standard Time"`, "20240304T090000Z", "20240304T090000Z"},
		{"floating value", outlookEasternObject, "DTSTART", "20240304T090000", "20240304T090000Z"},
		{"date", outlookEasternObject, "DTSTART;VALUE=DATE", "20240304", "20240304T000000Z"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := NewObjectTimes(tt.lines).ResolvePropertyTime(tt.keyPart, tt.value)
			if !ok {
				t.Fatalf("ResolvePropertyTime(%s:%s) ok = false", tt.keyPart, tt.value)
			}
			if want, _ := ParseDateTime(tt.want); !got.Equal(want) {
				t.Errorf("ResolvePropertyTime(%s:%s) = %s, want %s", tt.keyPart, tt.value, got.UTC().Format("20060102T150405Z"), tt.want)
			}
		})
	}
	if _, ok := NewObjectTimes(outlookEasternObject).ResolvePropertyTime("DTSTART", "not a date"); ok {
		t.Error("an unparseable value resolved")
	}
}

// The end a writer stores for an event is the end the reports read it with:
// its DTEND when that ends it, the end of a DATE start's day or the DATE-TIME
// start itself when it does not, and no end at all where a DURATION decides.
func TestStoredEventEnd(t *testing.T) {
	at := func(value string) *time.Time {
		parsed, err := ParseDateTime(value)
		if err != nil {
			t.Fatal(err)
		}
		return &parsed
	}
	tests := []struct {
		name        string
		start       *time.Time
		startIsDate bool
		end         *time.Time
		hasDuration bool
		want        *time.Time
	}{
		{"DTEND after DTSTART", at("20250301T100000Z"), false, at("20250301T110000Z"), false, at("20250301T110000Z")},
		{"DATE-TIME DTEND equal to DTSTART", at("20250301T100000Z"), false, at("20250301T100000Z"), false, at("20250301T100000Z")},
		{"DATE-TIME DTEND before DTSTART", at("20250301T100000Z"), false, at("20250301T080000Z"), false, at("20250301T100000Z")},
		{"DATE DTEND equal to DTSTART", at("20250301"), true, at("20250301"), false, at("20250302")},
		{"DATE DTEND before DTSTART", at("20250301"), true, at("20250226"), false, at("20250302")},
		{"unusable DTEND beside a DURATION", at("20250301T100000Z"), false, at("20250301T080000Z"), true, nil},
		{"unusable DATE DTEND beside a DURATION", at("20250301"), true, at("20250301"), true, nil},
		{"usable DTEND beside a DURATION", at("20250301T100000Z"), false, at("20250301T110000Z"), true, at("20250301T110000Z")},
		{"no DTEND", at("20250301T100000Z"), false, nil, false, at("20250301T100000Z")},
		{"no DTEND on a DATE start", at("20250301"), true, nil, false, at("20250302")},
		{"no DTEND beside a DURATION", at("20250301T100000Z"), false, nil, true, nil},
		{"no DTEND beside a DURATION on a DATE start", at("20250301"), true, nil, true, nil},
		{"no DTSTART", nil, false, at("20250301T110000Z"), false, at("20250301T110000Z")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := StoredEventEnd(tt.start, tt.startIsDate, tt.end, tt.hasDuration)
			if (got == nil) != (tt.want == nil) || got != nil && !got.Equal(*tt.want) {
				t.Errorf("StoredEventEnd = %v, want %v", got, tt.want)
			}
		})
	}
}

// countingOnsets is an ObservanceOnsets that counts the questions put to it
// and keeps one listing of each rule.
type countingOnsets struct {
	listings onsetListings
	asked    int
}

func (c *countingOnsets) LatestOnOrBefore(dtstart, before time.Time, rrule string) (time.Time, bool, bool) {
	c.asked++
	return c.listings.LatestOnOrBefore(dtstart, before, rrule)
}

// The recurrence-limit check and the recurrence bounds read an object's dates
// through the ObjectTimes the analysis read them with, so a definition's rules
// are listed once for the object, through the listings it shares, rather than
// once by each reader.
func TestRecurrenceReadersShareTheObjectsTimes(t *testing.T) {
	lines := UnfoldLines("BEGIN:VCALENDAR\r\nVERSION:2.0\r\n" +
		"BEGIN:VTIMEZONE\r\nTZID:Eastern Standard Time\r\n" +
		"BEGIN:STANDARD\r\nDTSTART:16010101T020000\r\nTZOFFSETFROM:-0400\r\nTZOFFSETTO:-0500\r\nRRULE:FREQ=YEARLY;INTERVAL=1;BYDAY=1SU;BYMONTH=11\r\nEND:STANDARD\r\n" +
		"BEGIN:DAYLIGHT\r\nDTSTART:16010101T020000\r\nTZOFFSETFROM:-0500\r\nTZOFFSETTO:-0400\r\nRRULE:FREQ=YEARLY;INTERVAL=1;BYDAY=2SU;BYMONTH=3\r\nEND:DAYLIGHT\r\n" +
		"END:VTIMEZONE\r\n" +
		"BEGIN:VEVENT\r\nUID:x\r\nDTSTART;TZID=\"Eastern Standard Time\":20240304T090000\r\n" +
		"DTEND;TZID=\"Eastern Standard Time\":20240304T100000\r\nRRULE:FREQ=WEEKLY;COUNT=10\r\n" +
		"EXDATE;TZID=\"Eastern Standard Time\":20240318T090000\r\nEND:VEVENT\r\n" +
		"BEGIN:VEVENT\r\nUID:x\r\nRECURRENCE-ID;TZID=\"Eastern Standard Time\":20240325T090000\r\n" +
		"DTSTART;TZID=\"Eastern Standard Time\":20240325T110000\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n")
	onsets := &countingOnsets{listings: onsetListings{}}
	times := NewObjectTimesSharingOnsets(lines, onsets)
	if _, ok := times.ResolvePropertyTime(`DTSTART;TZID="Eastern Standard Time"`, "20240304T090000"); !ok {
		t.Fatal("DTSTART does not resolve")
	}
	analyzed := onsets.asked

	exceeds, ok := RecurrenceSetExceedsLimitOf(times, MaxRecurrenceInstances)
	if wantExceeds, wantOK := RecurrenceSetExceedsLimitFromUnfoldedLines(lines, MaxRecurrenceInstances); exceeds != wantExceeds || ok != wantOK {
		t.Fatalf("RecurrenceSetExceedsLimitOf = %v, %v; the lines read alone give %v, %v", exceeds, ok, wantExceeds, wantOK)
	}
	if !ok || exceeds {
		t.Fatalf("RecurrenceSetExceedsLimitOf = %v, %v; want a valid set within the limit", exceeds, ok)
	}
	if onsets.asked == analyzed {
		t.Error("the recurrence-limit check read the zone's rules through listings of its own")
	}
	checked := onsets.asked

	if bounds := ConservativeRecurrenceBoundsOf(times); !bounds.Recurring || bounds.Start == nil {
		t.Fatalf("bounds = %+v", bounds)
	}
	if onsets.asked == checked {
		t.Error("the recurrence bounds read the zone's rules through listings of their own")
	}
	if got := len(onsets.listings); got != 2 {
		t.Errorf("%d rule listings, want one of each of the definition's 2 rules", got)
	}
}
