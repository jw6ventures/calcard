package dav

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/ical"
	"github.com/jw6ventures/calcard/internal/store"
)

// writableCalendarServer builds a server holding one writable calendar, together
// with the event repository the caller inspects to prove a rejected object was
// not stored and an accepted one was stored verbatim.
func writableCalendarServer() (*DavServer, *fakeEventRepo) {
	eventRepo := &fakeEventRepo{events: map[string]*store.Event{}}
	calRepo := &fakeCalendarRepo{accessible: []store.CalendarAccess{
		{Calendar: store.Calendar{ID: 1, UserID: 1, Name: "Test"}, Editor: true},
	}}
	return &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}, eventRepo
}

// buildVTimezone returns the smallest VTIMEZONE RFC 5545 §3.6.5 admits, so a
// case exercising a TZID parameter rule is not also exercising the §4.1 rule
// that every referenced TZID is defined.
func buildVTimezone(tzid string) string {
	return buildComponent("VTIMEZONE",
		"TZID:"+tzid,
		buildComponent("STANDARD",
			"DTSTART:19700101T000000",
			"TZOFFSETFROM:+0000",
			"TZOFFSETTO:+0000",
			"TZNAME:GMT"))
}

func putCalendarObject(t *testing.T, h *DavServer, name, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := newCalendarPutRequest("/dav/calendars/1/"+name, strings.NewReader(body))
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 1}))
	rr := httptest.NewRecorder()
	h.Put(rr, req)
	return rr
}

// Section 5.3.2.1 (CALDAV:valid-calendar-data): submitted data must be valid for
// its media type, which for iCalendar means the RFC 5545 content model and value
// grammar rather than balanced BEGIN and END lines alone. Each case below is
// well-formed as a nesting of components and invalid as iCalendar.
func TestRFC4791_ValidCalendarData_EnforcesICalendarGrammar(t *testing.T) {
	tests := map[string]string{
		"VCALENDAR without PRODID": "BEGIN:VCALENDAR\r\nVERSION:2.0\r\n" +
			buildVEvent("no-prodid") + "END:VCALENDAR\r\n",
		"VCALENDAR without VERSION": "BEGIN:VCALENDAR\r\nPRODID:" + testProdID + "\r\n" +
			buildVEvent("no-version") + "END:VCALENDAR\r\n",
		"VCALENDAR at another version": "BEGIN:VCALENDAR\r\nVERSION:1.0\r\nPRODID:" + testProdID + "\r\n" +
			buildVEvent("old-version") + "END:VCALENDAR\r\n",
		"non-Gregorian CALSCALE": "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:" + testProdID +
			"\r\nCALSCALE:JULIAN\r\n" + buildVEvent("calscale") + "END:VCALENDAR\r\n",
		"VEVENT without DTSTAMP": buildCalendarObject(buildComponent("VEVENT",
			"UID:no-dtstamp", "DTSTART:"+testDTStart)),
		"VEVENT without DTSTART in a METHOD-less object": buildCalendarObject(buildComponent("VEVENT",
			"UID:no-dtstart", "DTSTAMP:"+testDTStamp)),
		"repeated SUMMARY": buildCalendarObject(buildVEvent("twice",
			"SUMMARY:One", "SUMMARY:Two")),
		"DTEND alongside DURATION": buildCalendarObject(buildVEvent("both",
			"DTEND:20240601T110000Z", "DURATION:PT1H")),
		"VEVENT date types do not match": buildCalendarObject(buildVEvent("event-types",
			"DTSTART;VALUE=DATE:20240601", "DTEND:20240601T110000Z")),
		"VEVENT DTEND is not later than DTSTART": buildCalendarObject(buildVEvent("event-order",
			"DTSTART:20240601T110000Z", "DTEND:20240601T100000Z")),
		"malformed DURATION": buildCalendarObject(buildVEvent("bad-duration",
			"DURATION:not-a-duration")),
		"repeated RRULE": buildCalendarObject(buildVEvent("two-rules",
			"RRULE:FREQ=DAILY;COUNT=2", "RRULE:FREQ=WEEKLY;COUNT=2")),
		"malformed RRULE": buildCalendarObject(buildVEvent("bad-rule",
			"RRULE:FREQ")),
		"RRULE carrying COUNT and UNTIL": buildCalendarObject(buildVEvent("two-ends",
			"RRULE:FREQ=DAILY;COUNT=2;UNTIL=20240603T100000Z")),
		"RRULE with a frequency-forbidden BY part": buildCalendarObject(buildVEvent("bad-by-part",
			"RRULE:FREQ=WEEKLY;BYMONTHDAY=1")),
		"RRULE with an unknown rule part": buildCalendarObject(buildVEvent("unknown-rule-part",
			"RRULE:FREQ=DAILY;X-CALCARD=1")),
		"VTODO DURATION without DTSTART": buildCalendarObject(buildVTodo("orphan-duration",
			"DURATION:PT1H")),
		"VTODO date types do not match": buildCalendarObject(buildVTodo("todo-types",
			"DTSTART;VALUE=DATE:20240601", "DUE:20240601T110000Z")),
		"VTODO DUE is not later than DTSTART": buildCalendarObject(buildVTodo("todo-order",
			"DTSTART:20240601T110000Z", "DUE:20240601T100000Z")),
		"VALARM without ACTION": buildCalendarObject(buildVEvent("alarm",
			buildComponent("VALARM", "TRIGGER:-PT15M"))),
		"DISPLAY VALARM without DESCRIPTION": buildCalendarObject(buildVEvent("display-alarm",
			buildComponent("VALARM", "ACTION:DISPLAY", "TRIGGER:-PT15M"))),
		"VALARM REPEAT without DURATION": buildCalendarObject(buildVEvent("repeat-alarm",
			buildComponent("VALARM", "ACTION:DISPLAY", "DESCRIPTION:Alarm",
				"TRIGGER:-PT15M", "REPEAT:2"))),
		"AUDIO VALARM carrying DISPLAY-only DESCRIPTION": buildCalendarObject(buildVEvent("audio-description",
			buildComponent("VALARM", "ACTION:AUDIO", "TRIGGER:-PT15M", "DESCRIPTION:Alarm"))),
		"DISPLAY VALARM carrying AUDIO-only ATTACH": buildCalendarObject(buildVEvent("display-attach",
			buildComponent("VALARM", "ACTION:DISPLAY", "TRIGGER:-PT15M", "DESCRIPTION:Alarm",
				"ATTACH:https://example.test/alarm.wav"))),
		"VFREEBUSY carrying an RRULE": buildCalendarObject(buildVFreeBusy("recurring-freebusy",
			"RRULE:FREQ=DAILY;COUNT=2")),
		"VFREEBUSY DTSTART is not UTC": buildCalendarObject(buildVFreeBusy("freebusy-start",
			"DTSTART:20240601T100000")),
		"VFREEBUSY DTEND is not UTC": buildCalendarObject(buildVFreeBusy("freebusy-end",
			"DTEND:20240601T120000")),
		"VFREEBUSY DTEND is not later than DTSTART": buildCalendarObject(buildVFreeBusy("freebusy-order",
			"DTSTART:20240601T120000Z", "DTEND:20240601T110000Z")),
		"VALARM nested in a VJOURNAL": buildCalendarObject(buildVJournal("journal-alarm",
			buildComponent("VALARM", "ACTION:DISPLAY", "DESCRIPTION:Alarm", "TRIGGER:-PT15M"))),
		"content line with no value delimiter": buildCalendarObject(buildComponent("VEVENT",
			"UID:no-colon", "DTSTAMP:"+testDTStamp, "DTSTART:"+testDTStart, "SUMMARY")),
		"parameter with no value": buildCalendarObject(buildComponent("VEVENT",
			"UID:bare-param", "DTSTAMP:"+testDTStamp, "DTSTART;TZID:"+testDTStart)),
		"VEVENT property after VALARM": buildCalendarObject(buildComponent("VEVENT",
			"UID:late-event-property", "DTSTAMP:"+testDTStamp, "DTSTART:"+testDTStart,
			buildComponent("VALARM", "ACTION:DISPLAY", "DESCRIPTION:Alarm", "TRIGGER:-PT15M"),
			"SUMMARY:Too late")),
		"VTIMEZONE property after observance": buildCalendarObject(
			buildComponent("VTIMEZONE",
				buildComponent("STANDARD", "DTSTART:19700101T000000", "TZOFFSETFROM:+0000", "TZOFFSETTO:+0000"),
				"TZID:Example/Late"),
			buildVEvent("late-timezone-property")),
		"VTIMEZONE without an observance": buildCalendarObject(
			buildComponent("VTIMEZONE", "TZID:Example/Zone"),
			buildVEvent("no-observance")),
	}

	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			h, eventRepo := writableCalendarServer()

			rr := putCalendarObject(t, h, "grammar.ics", body)

			assertErrorConditions(t, rr, http.StatusForbidden, calQN("valid-calendar-data"))
			if len(eventRepo.events) != 0 {
				t.Fatalf("rejected object was stored: %#v", eventRepo.events)
			}
		})
	}
}

func TestRFC4791_NonStandardComponentAcceptedWhenCollectionAdvertisesIt(t *testing.T) {
	eventRepo := &fakeEventRepo{events: map[string]*store.Event{}}
	calRepo := &fakeCalendarRepo{accessible: []store.CalendarAccess{{
		Calendar: store.Calendar{
			ID:                  1,
			UserID:              1,
			Name:                "Extensions",
			SupportedComponents: []string{"X-CALCARD-THING"},
		},
		Editor: true,
	}}}
	h := &DavServer{store: &store.Store{Calendars: calRepo, Events: eventRepo}}
	body := buildCalendarObject(buildComponent("X-CALCARD-THING",
		"UID:extension-object", "X-CALCARD-VALUE:preserve me"))

	rr := putCalendarObject(t, h, "extension.ics", body)

	if rr.Code != http.StatusCreated {
		t.Fatalf("PUT X-component = %d, want 201: %s", rr.Code, rr.Body.String())
	}
	stored := eventRepo.events["1:extension-object"]
	if stored == nil || stored.RawICAL != body {
		t.Fatalf("stored X-component = %#v, want submitted object preserved", stored)
	}
}

func TestRFC4791_MkcalendarAcceptsNonStandardSupportedComponent(t *testing.T) {
	components, ok := parseSupportedComponentSet(
		`<cal:comp xmlns:cal="urn:ietf:params:xml:ns:caldav" name="X-CALCARD-THING"/>`,
	)
	if !ok || len(components) != 1 || components[0] != "X-CALCARD-THING" {
		t.Fatalf("parseSupportedComponentSet(X-CALCARD-THING) = %v, %t", components, ok)
	}
}

func TestRFC4791_DateLimitsUseSubmittedVTimezoneDefinition(t *testing.T) {
	minDate, _ := ical.DateLimits()
	for _, tzid := range []string{"Custom/Plus14", "America/New_York"} {
		t.Run(tzid, func(t *testing.T) {
			h, eventRepo := writableCalendarServer()
			body := buildCalendarObject(
				buildComponent("VTIMEZONE",
					"TZID:"+tzid,
					buildComponent("STANDARD",
						"DTSTART:19700101T000000",
						"TZOFFSETFROM:+1400",
						"TZOFFSETTO:+1400")),
				buildVEvent("before-min-in-submitted-zone",
					"DTSTART;TZID="+tzid+":"+minDate.Format("20060102T150405")))

			rr := putCalendarObject(t, h, "submitted-zone.ics", body)

			assertErrorConditions(t, rr, http.StatusForbidden, calQN("min-date-time"))
			if len(eventRepo.events) != 0 {
				t.Fatalf("object before min-date-time was stored: %#v", eventRepo.events)
			}
		})
	}
}

func TestSubmittedVTimezoneResolverAppliesRecurringObservances(t *testing.T) {
	body := buildCalendarObject(
		buildComponent("VTIMEZONE",
			"TZID:Example/Eastern",
			buildComponent("STANDARD",
				"DTSTART:19701101T020000",
				"TZOFFSETFROM:-0400",
				"TZOFFSETTO:-0500",
				"RRULE:FREQ=YEARLY;BYMONTH=11;BYDAY=1SU"),
			buildComponent("DAYLIGHT",
				"DTSTART:19700308T020000",
				"TZOFFSETFROM:-0500",
				"TZOFFSETTO:-0400",
				"RRULE:FREQ=YEARLY;BYMONTH=3;BYDAY=2SU")),
		buildVEvent("timezone-observances", "DTSTART;TZID=Example/Eastern:20240115T100000"))
	root, err := parseICalendarObject(body)
	if err != nil {
		t.Fatalf("parse calendar: %v", err)
	}
	if fault := validateCalendarObject(root); fault != nil {
		t.Fatalf("valid calendar rejected: %#v", fault)
	}

	tests := []struct {
		wall time.Time
		want time.Duration
	}{
		{wall: time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC), want: -5 * time.Hour},
		{wall: time.Date(2024, 7, 15, 10, 0, 0, 0, time.UTC), want: -4 * time.Hour},
	}
	for _, tt := range tests {
		got, ok := submittedTimezoneOffset(root, "Example/Eastern", tt.wall)
		if !ok || got != tt.want {
			t.Errorf("submittedTimezoneOffset(%s) = %s, %t; want %s", tt.wall, got, ok, tt.want)
		}
	}
}

// Sections 3.3.4 and 3.3.5 of RFC 5545 define three spellings of a DATE or
// DATE-TIME and no more: a bare date, a floating date-time, and a date-time in
// UTC. A numeric UTC offset belongs to no iCalendar value type, so it is not
// calendar data whatever a lenient parser makes of it.
func TestRFC4791_ValidCalendarData_RejectsIllFormedDateValues(t *testing.T) {
	tests := map[string]string{
		"numeric offset on DTSTART":         "DTSTART:20240601T100000-0700",
		"colon offset on DTSTART":           "DTSTART:20240601T100000-07:00",
		"extended format on DTSTART":        "DTSTART:2024-06-01T10:00:00Z",
		"bare DATE without VALUE=DATE":      "DTSTART:20240601",
		"VALUE=DATE with a time":            "DTSTART;VALUE=DATE:20240601T100000Z",
		"unknown VALUE on DTSTART":          "DTSTART;VALUE=BOGUS:20240601T100000Z",
		"VALUE=DATE on UTC-only DTSTAMP":    "DTSTAMP;VALUE=DATE:20240601T000000Z",
		"TZID on a UTC value":               "DTSTART;TZID=Example/Zone:20240601T100000Z",
		"TZID on a DATE value":              "DTSTART;VALUE=DATE;TZID=Example/Zone:20240601",
		"impossible month":                  "DTSTART:20241301T100000Z",
		"floating DTSTAMP":                  "DTSTAMP:20240601T100000",
		"numeric offset in an RDATE":        "RDATE:20240602T100000+0200",
		"unparseable VALARM TRIGGER":        "TRIGGER:not-a-duration",
		"floating FREEBUSY period":          "FREEBUSY:20240601T100000/20240601T110000",
		"FREEBUSY period with no slash":     "FREEBUSY:20240601T100000Z",
		"numeric offset in typed extension": "X-CALCARD-DATE;VALUE=DATE-TIME:20240601T100000-0700",
	}

	for name, line := range tests {
		t.Run(name, func(t *testing.T) {
			var body string
			switch {
			case strings.HasPrefix(line, "FREEBUSY"):
				body = buildCalendarObject(buildVFreeBusy("dates", line))
			case strings.HasPrefix(line, "TRIGGER"):
				body = buildCalendarObject(buildVEvent("dates",
					buildComponent("VALARM", "ACTION:DISPLAY", "DESCRIPTION:Alarm", line)))
			case strings.Contains(line, "TZID="):
				body = buildCalendarObject(buildVTimezone("Example/Zone"), buildVEvent("dates", line))
			default:
				body = buildCalendarObject(buildVEvent("dates", line))
			}
			h, eventRepo := writableCalendarServer()

			rr := putCalendarObject(t, h, "dates.ics", body)

			assertErrorConditions(t, rr, http.StatusForbidden, calQN("valid-calendar-data"))
			if len(eventRepo.events) != 0 {
				t.Fatalf("rejected object was stored: %#v", eventRepo.events)
			}
		})
	}
}

func TestRFC4791_DateLimitsIncludeExplicitlyTypedExtensionProperties(t *testing.T) {
	h, eventRepo := writableCalendarServer()
	body := buildCalendarObject(buildVEvent("extension-before-min",
		"X-CALCARD-DATE;VALUE=DATE-TIME:18991231T235959Z"))

	rr := putCalendarObject(t, h, "extension-before-min.ics", body)

	assertErrorConditions(t, rr, http.StatusForbidden, calQN("min-date-time"))
	if len(eventRepo.events) != 0 {
		t.Fatalf("extension date before min-date-time was stored: %#v", eventRepo.events)
	}
}

func TestRFC4791_ValidCalendarData_RejectsInvalidPeriods(t *testing.T) {
	tests := map[string]string{
		"FREEBUSY end before start": buildCalendarObject(buildVFreeBusy("period",
			"FREEBUSY:20240601T110000Z/20240601T100000Z")),
		"FREEBUSY end equal to start": buildCalendarObject(buildVFreeBusy("period",
			"FREEBUSY:20240601T100000Z/20240601T100000Z")),
		"FREEBUSY zero duration": buildCalendarObject(buildVFreeBusy("period",
			"FREEBUSY:20240601T100000Z/PT0S")),
		"FREEBUSY negative duration": buildCalendarObject(buildVFreeBusy("period",
			"FREEBUSY:20240601T100000Z/-PT1H")),
		"RDATE end before start": buildCalendarObject(buildVEvent("period",
			"RDATE;VALUE=PERIOD:20240601T110000Z/20240601T100000Z")),
	}

	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			h, eventRepo := writableCalendarServer()

			rr := putCalendarObject(t, h, "period.ics", body)

			assertErrorConditions(t, rr, http.StatusForbidden, calQN("valid-calendar-data"))
			if len(eventRepo.events) != 0 {
				t.Fatalf("rejected object was stored: %#v", eventRepo.events)
			}
		})
	}
}

func TestRFC4791_ValidCalendarData_AcceptsRDatePeriods(t *testing.T) {
	tests := map[string]string{
		"explicit end": "RDATE;VALUE=PERIOD:20240602T100000Z/20240602T120000Z",
		"duration end": "RDATE;VALUE=PERIOD:20240602T100000Z/PT2H",
	}

	for name, rdate := range tests {
		t.Run(name, func(t *testing.T) {
			h, eventRepo := writableCalendarServer()
			body := buildCalendarObject(buildVEvent("period", rdate))

			rr := putCalendarObject(t, h, "period.ics", body)

			if rr.Code != http.StatusCreated {
				t.Fatalf("PUT with a valid RDATE PERIOD = %d, want 201: %s", rr.Code, rr.Body.String())
			}
			stored := eventRepo.events[eventRepo.key(1, "period")]
			if stored == nil || stored.RawICAL != body {
				t.Fatalf("stored RDATE PERIOD = %#v, want submitted object preserved", stored)
			}
		})
	}
}

func TestRFC4791_ValidCalendarData_AcceptsPositiveLeapSecond(t *testing.T) {
	h, eventRepo := writableCalendarServer()
	body := buildCalendarObject(buildVEvent("leap-second",
		"DTSTAMP:19970630T235960Z",
		"DTSTART:19970630T235960Z"))

	rr := putCalendarObject(t, h, "leap-second.ics", body)

	if rr.Code != http.StatusCreated {
		t.Fatalf("PUT with a positive leap second = %d, want 201: %s", rr.Code, rr.Body.String())
	}
	stored := eventRepo.events[eventRepo.key(1, "leap-second")]
	if stored == nil || stored.RawICAL != body {
		t.Fatalf("stored leap-second event = %#v, want submitted object preserved", stored)
	}
}

// Section 4.1: a VTIMEZONE is specified for each unique TZID parameter value the
// object uses. A reference with no definition leaves the stored resource
// unreadable to the client that fetches it, which is the restriction
// CALDAV:valid-calendar-object-resource carries.
func TestRFC4791_ValidCalendarObject_RequiresVTimezoneForEachTZID(t *testing.T) {
	t.Run("reference without a definition is rejected", func(t *testing.T) {
		h, eventRepo := writableCalendarServer()
		body := buildCalendarObject(buildVEvent("undefined-tzid",
			"DTSTART;TZID=Example/Zone:20240601T100000"))

		rr := putCalendarObject(t, h, "tz.ics", body)

		assertErrorConditions(t, rr, http.StatusForbidden, calQN("valid-calendar-object-resource"))
		if len(eventRepo.events) != 0 {
			t.Fatalf("rejected object was stored: %#v", eventRepo.events)
		}
	})

	t.Run("one definition per referenced TZID is accepted", func(t *testing.T) {
		h, _ := writableCalendarServer()
		body := buildCalendarObject(
			buildVTimezone("Example/Zone"),
			buildVTimezone("Example/Other"),
			buildVEvent("defined-tzids",
				"DTSTART;TZID=Example/Zone:20240601T100000",
				"DTEND;TZID=Example/Other:20240601T110000"))

		rr := putCalendarObject(t, h, "tz.ics", body)

		if rr.Code != http.StatusCreated {
			t.Fatalf("PUT with a VTIMEZONE per TZID = %d, want 201; body: %s", rr.Code, rr.Body.String())
		}
	})

	t.Run("a second definition of one TZID is rejected", func(t *testing.T) {
		h, _ := writableCalendarServer()
		body := buildCalendarObject(
			buildVTimezone("Example/Zone"),
			buildVTimezone("Example/Zone"),
			buildVEvent("duplicate-tzid", "DTSTART;TZID=Example/Zone:20240601T100000"))

		rr := putCalendarObject(t, h, "tz.ics", body)

		assertErrorConditions(t, rr, http.StatusForbidden, calQN("valid-calendar-object-resource"))
	})
}

// Section 4.1 describes a recurrence set as the master component plus its
// overridden instances, and admits a resource carrying only overrides: the
// components sharing one UID are the ones that go in one resource, however many
// of them the client is sending.
func TestRFC4791_ValidCalendarObject_RecurrenceSetShapes(t *testing.T) {
	t.Run("overrides with no master are accepted", func(t *testing.T) {
		h, eventRepo := writableCalendarServer()
		body := buildCalendarObject(
			buildVEvent("override-only",
				"RECURRENCE-ID:20240601T100000Z", "DTSTART:20240601T110000Z"),
			buildVEvent("override-only",
				"RECURRENCE-ID:20240602T100000Z", "DTSTART:20240602T113000Z"))

		rr := putCalendarObject(t, h, "overrides.ics", body)

		if rr.Code != http.StatusCreated {
			t.Fatalf("PUT of an override-only recurrence set = %d, want 201; body: %s", rr.Code, rr.Body.String())
		}
		if len(eventRepo.events) != 1 {
			t.Fatalf("stored events = %d, want the one resource the overrides share", len(eventRepo.events))
		}
	})

	t.Run("two masters sharing a UID are rejected", func(t *testing.T) {
		h, _ := writableCalendarServer()
		body := buildCalendarObject(
			buildVEvent("two-masters", "DTSTART:20240601T100000Z"),
			buildVEvent("two-masters", "DTSTART:20240602T100000Z"))

		rr := putCalendarObject(t, h, "masters.ics", body)

		assertErrorConditions(t, rr, http.StatusForbidden, calQN("valid-calendar-object-resource"))
	})

	t.Run("two overrides of one instance are rejected", func(t *testing.T) {
		h, _ := writableCalendarServer()
		body := buildCalendarObject(
			buildVEvent("clashing", "DTSTART:20240601T100000Z"),
			buildVEvent("clashing", "RECURRENCE-ID:20240602T100000Z", "DTSTART:20240602T110000Z"),
			buildVEvent("clashing", "RECURRENCE-ID:20240602T100000Z", "DTSTART:20240602T120000Z"))

		rr := putCalendarObject(t, h, "clash.ics", body)

		assertErrorConditions(t, rr, http.StatusForbidden, calQN("valid-calendar-object-resource"))
	})

	t.Run("override value type must match the master DTSTART", func(t *testing.T) {
		h, _ := writableCalendarServer()
		body := buildCalendarObject(
			buildVEvent("type-mismatch", "DTSTART;VALUE=DATE:20240601"),
			buildVEvent("type-mismatch", "RECURRENCE-ID:20240602T100000Z", "DTSTART:20240602T110000Z"))

		rr := putCalendarObject(t, h, "type-mismatch.ics", body)

		assertErrorConditions(t, rr, http.StatusForbidden, calQN("valid-calendar-data"))
	})

	t.Run("override TZID must match the master DTSTART", func(t *testing.T) {
		h, _ := writableCalendarServer()
		body := buildCalendarObject(
			buildVTimezone("Example/Zone"),
			buildVEvent("tzid-mismatch", "DTSTART;TZID=Example/Zone:20240601T100000"),
			buildVEvent("tzid-mismatch", "RECURRENCE-ID:20240602T100000", "DTSTART;TZID=Example/Zone:20240602T110000"))

		rr := putCalendarObject(t, h, "tzid-mismatch.ics", body)

		assertErrorConditions(t, rr, http.StatusForbidden, calQN("valid-calendar-data"))
	})

	t.Run("equivalent recurrence instants are duplicate overrides", func(t *testing.T) {
		h, _ := writableCalendarServer()
		body := buildCalendarObject(
			buildComponent("VTIMEZONE",
				"TZID:Example/Forward",
				buildComponent("DAYLIGHT",
					"DTSTART:20240601T110000", "TZOFFSETFROM:+0000", "TZOFFSETTO:+0100")),
			buildVEvent("normalized-clash",
				"RECURRENCE-ID;TZID=Example/Forward:20240601T100000", "DTSTART:20240601T120000Z"),
			buildVEvent("normalized-clash",
				"RECURRENCE-ID;TZID=Example/Forward:20240601T110000", "DTSTART:20240601T130000Z"))

		rr := putCalendarObject(t, h, "normalized-clash.ics", body)

		assertErrorConditions(t, rr, http.StatusForbidden, calQN("valid-calendar-object-resource"))
	})

	// The one-component-type rule of §4.1 holds across a recurrence set too: a
	// VTODO override of a VEVENT master shares its UID and is still two types
	// of calendar component in one resource.
	t.Run("an override of another component type is rejected", func(t *testing.T) {
		h, _ := writableCalendarServer()
		body := buildCalendarObject(
			buildVEvent("mixed-recurrence", "DTSTART:20240601T100000Z"),
			buildVTodo("mixed-recurrence", "RECURRENCE-ID:20240602T100000Z"))

		rr := putCalendarObject(t, h, "mixed.ics", body)

		assertErrorConditions(t, rr, http.StatusForbidden, calQN("valid-calendar-object-resource"))
	})
}

// Section 5.3.3: non-standard components, properties and parameters submitted
// through PUT are supported and preserved. The stored octets are compared
// against the submitted ones, because §5.3.4's strong ETag says they are equal.
func TestRFC4791_PutPreservesNonStandardCalendarData(t *testing.T) {
	h, eventRepo := writableCalendarServer()
	body := buildCalendarObject(buildVEvent("extensions",
		"X-CALCARD-TEST-FLAG;X-CALCARD-TEST-PARAM=kept:value",
		"X-CALCARD-DATE;VALUE=DATE-TIME:20240601T100000Z",
		"SUMMARY;X-CALCARD-TEST-PARAM=kept:Extended",
		buildComponent("X-CALCARD-TEST-COMPONENT", "X-CALCARD-TEST-INNER:nested")))

	rr := putCalendarObject(t, h, "extensions.ics", body)

	if rr.Code != http.StatusCreated {
		t.Fatalf("PUT carrying non-standard content = %d, want 201; body: %s", rr.Code, rr.Body.String())
	}
	stored := eventRepo.events[eventRepo.key(1, "extensions")]
	if stored == nil {
		t.Fatal("object was not stored")
	}
	if stored.RawICAL != body {
		t.Fatalf("stored octets = %q, want the submitted %q", stored.RawICAL, body)
	}
}

// Section 8.5.1: inline attachments are supported. The size limit a server puts
// on them is expressed through CALDAV:max-resource-size and nothing else, so an
// attachment inside that limit is stored and one past it fails that precondition
// rather than a separate attachment rule.
func TestRFC4791_PutSupportsInlineAttachmentsBoundedByMaxResourceSize(t *testing.T) {
	inline := func(payloadBytes int) string {
		return buildCalendarObject(buildVEvent("attached",
			"ATTACH;FMTTYPE=text/plain;ENCODING=BASE64;VALUE=BINARY:"+strings.Repeat("QQ==", payloadBytes/4)))
	}

	t.Run("an inline attachment inside the limit is stored", func(t *testing.T) {
		h, eventRepo := writableCalendarServer()
		body := inline(4096)

		rr := putCalendarObject(t, h, "attach.ics", body)

		if rr.Code != http.StatusCreated {
			t.Fatalf("PUT with an inline attachment = %d, want 201; body: %s", rr.Code, rr.Body.String())
		}
		stored := eventRepo.events[eventRepo.key(1, "attached")]
		if stored == nil || stored.RawICAL != body {
			t.Fatalf("inline attachment was not preserved: %#v", stored)
		}
	})

	t.Run("an inline attachment past the limit fails max-resource-size", func(t *testing.T) {
		h, eventRepo := writableCalendarServer()

		rr := putCalendarObject(t, h, "attach.ics", inline(int(maxDAVBodyBytes)+4))

		assertErrorConditions(t, rr, http.StatusForbidden, calQN("max-resource-size"))
		if len(eventRepo.events) != 0 {
			t.Fatalf("rejected object was stored: %#v", eventRepo.events)
		}
	})
}

// Section 5.3.3 (RFC 2445 §4.2): a non-standard property the server itself puts
// into calendar data carries a vendor id, so it cannot collide with another
// implementation's property of the same name. The generated birthday collection
// is the only calendar data CalCard authors.
func TestRFC4791_ServerAuthoredNonStandardPropertiesCarryVendorID(t *testing.T) {
	birthday := time.Date(1990, 5, 15, 0, 0, 0, 0, time.UTC)
	name := "Jane Doe"
	contactRepo := &fakeContactRepo{contacts: map[string]*store.Contact{
		"1:contact": {AddressBookID: 1, UID: "contact", DisplayName: &name, Birthday: &birthday},
	}}
	h := &DavServer{store: &store.Store{Contacts: contactRepo}}

	events, err := h.generateBirthdayEvents(context.Background(), 1)
	if err != nil {
		t.Fatalf("generateBirthdayEvents() error = %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("generated events = %d, want 1", len(events))
	}

	root, err := parseICalendarObject(events[0].RawICAL)
	if err != nil {
		t.Fatalf("parse generated birthday event: %v", err)
	}
	walkICalNodes(root, func(node *icalNode) {
		for _, property := range node.properties {
			if !strings.HasPrefix(property.name, "X-") {
				continue
			}
			// RFC 2445 §4.2 spells the shape as "X-" vendor id "-" name, so a
			// vendor id means at least two hyphen-separated parts after "X-".
			if len(strings.SplitN(strings.TrimPrefix(property.name, "X-"), "-", 2)) < 2 {
				t.Errorf("non-standard property %q carries no vendor id", property.name)
			}
		}
	})
}
