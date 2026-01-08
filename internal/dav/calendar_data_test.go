package dav

import (
	"strings"
	"testing"
)

func TestFilterICalendarDataKeepsAllComponentsWhenVCalendarOnly(t *testing.T) {
	raw := strings.Join([]string{
		"BEGIN:VCALENDAR",
		"VERSION:2.0",
		"PRODID:-//CalCard//EN",
		"BEGIN:VTIMEZONE",
		"TZID:UTC",
		"END:VTIMEZONE",
		"BEGIN:VEVENT",
		"UID:event-1",
		"DTSTART:20240101T090000Z",
		"DTEND:20240101T100000Z",
		"SUMMARY:Test Event",
		"END:VEVENT",
		"END:VCALENDAR",
		"",
	}, "\r\n")

	calData := &calendarDataEl{
		Comp: []calendarComp{
			{Name: "VCALENDAR"},
		},
	}

	filtered := filterICalendarData(raw, calData)

	if !strings.Contains(filtered, "BEGIN:VEVENT") {
		t.Fatalf("expected VEVENT to remain in filtered data, got: %s", filtered)
	}
	if !strings.Contains(filtered, "BEGIN:VTIMEZONE") {
		t.Fatalf("expected VTIMEZONE to remain in filtered data, got: %s", filtered)
	}
	if !strings.Contains(filtered, "BEGIN:VCALENDAR") {
		t.Fatalf("expected VCALENDAR wrapper to remain in filtered data, got: %s", filtered)
	}
}

func TestFilterICalendarDataAllowsTimezoneOnly(t *testing.T) {
	raw := strings.Join([]string{
		"BEGIN:VCALENDAR",
		"VERSION:2.0",
		"BEGIN:VTIMEZONE",
		"TZID:UTC",
		"END:VTIMEZONE",
		"BEGIN:VEVENT",
		"UID:event-1",
		"DTSTART:20240101T090000Z",
		"DTEND:20240101T100000Z",
		"SUMMARY:Test Event",
		"END:VEVENT",
		"END:VCALENDAR",
		"",
	}, "\r\n")

	calData := &calendarDataEl{
		Comp: []calendarComp{
			{
				Name: "VCALENDAR",
				Comp: []calendarComp{
					{Name: "VTIMEZONE"},
				},
			},
		},
	}

	filtered := filterICalendarData(raw, calData)

	if strings.Contains(filtered, "BEGIN:VEVENT") {
		t.Fatalf("expected VEVENT to be stripped, got: %s", filtered)
	}
	if !strings.Contains(filtered, "BEGIN:VTIMEZONE") {
		t.Fatalf("expected VTIMEZONE to remain in filtered data, got: %s", filtered)
	}
	if !strings.Contains(filtered, "BEGIN:VCALENDAR") {
		t.Fatalf("expected VCALENDAR wrapper to remain in filtered data, got: %s", filtered)
	}
}

func TestFilterICalendarDataPropOnlyFiltersProperties(t *testing.T) {
	raw := strings.Join([]string{
		"BEGIN:VCALENDAR",
		"VERSION:2.0",
		"BEGIN:VTIMEZONE",
		"TZID:UTC",
		"END:VTIMEZONE",
		"BEGIN:VEVENT",
		"UID:event-1",
		"DTSTART:20240101T090000Z",
		"SUMMARY:Test Event",
		"END:VEVENT",
		"END:VCALENDAR",
		"",
	}, "\r\n")

	calData := &calendarDataEl{
		Prop: []calendarProp{
			{Name: "UID"},
		},
	}

	filtered := filterICalendarData(raw, calData)

	if !strings.Contains(filtered, "UID:event-1") {
		t.Fatalf("expected UID to remain in filtered data, got: %s", filtered)
	}
	if strings.Contains(filtered, "SUMMARY:") {
		t.Fatalf("expected SUMMARY to be filtered, got: %s", filtered)
	}
	if strings.Contains(filtered, "DTSTART:") {
		t.Fatalf("expected DTSTART to be filtered, got: %s", filtered)
	}
}

func TestFilterICalendarDataHandlesMixedCaseBeginEnd(t *testing.T) {
	raw := strings.Join([]string{
		"begin:vcalendar",
		"VERSION:2.0",
		"BEGIN:VTIMEZONE",
		"TZID:UTC",
		"END:VTIMEZONE",
		"Begin:Vevent",
		"UID:event-1",
		"SUMMARY:Test Event",
		"End:Vevent",
		"end:vcalendar",
		"",
	}, "\r\n")

	calData := &calendarDataEl{
		Comp: []calendarComp{
			{
				Name: "VCALENDAR",
				Comp: []calendarComp{
					{Name: "VEVENT"},
				},
			},
		},
	}

	filtered := filterICalendarData(raw, calData)

	if !strings.Contains(filtered, "BEGIN:VEVENT") && !strings.Contains(filtered, "Begin:Vevent") {
		t.Fatalf("expected VEVENT to remain in filtered data, got: %s", filtered)
	}
	if strings.Contains(filtered, "BEGIN:VTIMEZONE") {
		t.Fatalf("expected VTIMEZONE to be stripped, got: %s", filtered)
	}
}
