package dav

import (
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/ical"
)

// fidelityZones are zones whose rules stress a VTIMEZONE: southern-hemisphere
// daylight time, transitions at 24:00, a half-hour zone, Ramadan suspensions
// of daylight time, a 45-minute offset, rules a zone abandoned, negative
// daylight saving, and a zone that never changes.
var fidelityZones = []string{
	"Europe/Berlin", "America/New_York", "Australia/Sydney", "America/Santiago", "Asia/Kolkata",
	"Africa/Casablanca", "Pacific/Chatham", "Asia/Tehran", "Europe/Dublin", "Etc/GMT+5",
}

// hostOffsetChange is one change of a host zone's offset, as the test reads it
// from the time package independently of the generator.
type hostOffsetChange struct {
	at       time.Time
	from, to time.Duration
}

func hostOffsetChanges(loc *time.Location, from, through time.Time) []hostOffsetChange {
	var changes []hostOffsetChange
	offsetAt := func(instant time.Time) time.Duration {
		_, offset := instant.In(loc).Zone()
		return time.Duration(offset) * time.Second
	}
	for instant := from; instant.Before(through); {
		_, end := instant.In(loc).ZoneBounds()
		if end.IsZero() {
			break
		}
		if !end.After(instant) {
			// The time package can end a period before the instant asked
			// about; the next day resumes the walk.
			end = instant.Add(24 * time.Hour)
		}
		if before, after := offsetAt(end.Add(-time.Second)), offsetAt(end); before != after {
			changes = append(changes, hostOffsetChange{at: end.UTC(), from: before, to: after})
		}
		instant = end
	}
	return changes
}

// fidelitySamples is a wall clock every stride hours from first through last,
// a stride prime to 24 reaching every hour of the day in turn, and every
// quarter hour within two hours of each wall clock at which the host zone's
// offset changes.
func fidelitySamples(changes []hostOffsetChange, first, last time.Time, stride int) []time.Time {
	var samples []time.Time
	for wall := first; !wall.After(last); wall = wall.Add(time.Duration(stride) * time.Hour) {
		samples = append(samples, wall)
	}
	for _, change := range changes {
		for _, onset := range []time.Time{change.at.Add(change.from), change.at.Add(change.to)} {
			for step := -8; step <= 8; step++ {
				samples = append(samples, onset.Add(time.Duration(step)*15*time.Minute))
			}
		}
	}
	return samples
}

// RFC 4791 §4.1 completes an object naming a zone only the host defines with
// the host's definition, and the matcher then reads the object's values by that
// definition rather than by the host. The definition is faithful if every wall
// clock in the span, out to CALDAV:max-date-time where an open-ended series
// reaches, is placed where the host's zone places it -- including the readings
// a transition skips or repeats, which both read as RFC 5545 §3.3.5 directs.
func TestInsertedVTimezonePlacesEveryWallClockAsTheHostZoneDoes(t *testing.T) {
	minDate, maxDate := ical.DateLimits()
	spans := []struct {
		name   string
		first  time.Time
		stride int
	}{
		{"from 2026", time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), 5},
		{"from min-date-time", minDate, 23},
	}
	if testing.Short() {
		for i := range spans {
			spans[i].stride = 25
		}
	}
	for _, tzid := range fidelityZones {
		t.Run(tzid, func(t *testing.T) {
			t.Parallel()
			loc, err := time.LoadLocation(tzid)
			if err != nil {
				t.Skipf("tzdata unavailable: %v", err)
			}
			host := newCalendarTimeRangeMatcher("", &icalNode{name: "VCALENDAR"}, floatingZone{})
			for _, span := range spans {
				definition, ok := ical.VTimezone(tzid, span.first, maxDate)
				if !ok {
					t.Fatalf("%s: no definition generated", span.name)
				}
				// A client reads its times by the definition; the server reads the
				// host's database instead, so the definition is read as a client's.
				raw := buildCalendarObject(clientReading(definition), buildComponent("VEVENT", "UID:e", "DTSTART;TZID="+tzid+":20260105T090000"))
				root, err := parseICalendarObject(raw)
				if err != nil {
					t.Fatalf("%s: definition does not parse: %v", span.name, err)
				}
				defined := newCalendarTimeRangeMatcher(raw, root, floatingZone{})
				observances := defined.timezoneObservances(tzid)

				changes := hostOffsetChanges(loc, span.first.Add(-48*time.Hour), maxDate.Add(48*time.Hour))
				var skipped, repeated int
				for _, wall := range fidelitySamples(changes, span.first, maxDate, span.stride) {
					value := wall.Format("20060102T150405")
					want, ok := host.resolveInstant(tzid, value, icalFloatingDateTime)
					if !ok {
						t.Fatalf("host does not resolve %s", value)
					}
					offset, ok := observanceOffsetAtWall(observances, wall)
					if !ok {
						t.Fatalf("%s: definition does not resolve %s", span.name, value)
					}
					if got := wall.Add(-offset); !got.Equal(want) {
						t.Errorf("%s: %s placed at %s, host places it at %s", span.name, value, got, want)
					}
					switch _, kind := transitionHolding(changes, wall); kind {
					case "skipped":
						skipped++
					case "repeated":
						repeated++
					}
				}
				t.Logf("%s %s: %d transitions; %d skipped and %d repeated readings among those compared",
					tzid, span.name, len(changes), skipped, repeated)
			}
		})
	}
}

// transitionHolding is the change whose skipped or repeated wall clocks hold
// wall, and which of the two wall is.
func transitionHolding(changes []hostOffsetChange, wall time.Time) (hostOffsetChange, string) {
	for _, change := range changes {
		low, high := change.at.Add(min(change.from, change.to)), change.at.Add(max(change.from, change.to))
		if wall.Before(low) || !wall.Before(high) {
			continue
		}
		if change.to > change.from {
			return change, "skipped"
		}
		return change, "repeated"
	}
	return hostOffsetChange{}, ""
}

// Every zone the host defines yields a definition, and each one places the
// wall clocks either side of and within every transition from
// CALDAV:min-date-time to CALDAV:max-date-time where the host's zone does.
func TestEveryHostZoneHasAFaithfulVTimezone(t *testing.T) {
	data, err := os.ReadFile("/usr/share/zoneinfo/tzdata.zi")
	if err != nil {
		t.Skipf("host zone list unavailable: %v", err)
	}
	var zones []string
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		switch {
		case len(fields) >= 2 && fields[0] == "Z":
			zones = append(zones, fields[1])
		case len(fields) >= 3 && fields[0] == "L":
			zones = append(zones, fields[2])
		}
	}
	slices.Sort(zones)
	minDate, maxDate := ical.DateLimits()
	host := newCalendarTimeRangeMatcher("", &icalNode{name: "VCALENDAR"}, floatingZone{})
	for _, tzid := range zones {
		loc, err := time.LoadLocation(tzid)
		if err != nil {
			continue
		}
		definition, ok := ical.VTimezone(tzid, minDate, maxDate)
		if !ok {
			t.Errorf("%s: no definition generated", tzid)
			continue
		}
		// Read as a client reads it, by the definition rather than the host.
		raw := buildCalendarObject(clientReading(definition), buildComponent("VEVENT", "UID:e", "DTSTART;TZID="+tzid+":20260105T090000"))
		root, err := parseICalendarObject(raw)
		if err != nil {
			t.Errorf("%s: definition does not parse: %v", tzid, err)
			continue
		}
		if fault := validateCalendarObject(root); fault != nil {
			t.Errorf("%s: definition is refused: %+v", tzid, fault)
			continue
		}
		observances := newCalendarTimeRangeMatcher(raw, root, floatingZone{}).timezoneObservances(tzid)
		changes := hostOffsetChanges(loc, minDate.Add(-48*time.Hour), maxDate.Add(48*time.Hour))
		for _, change := range changes {
			low, high := change.at.Add(min(change.from, change.to)), change.at.Add(max(change.from, change.to))
			for _, wall := range []time.Time{
				low.Add(-time.Hour), low.Add(-time.Second), low, low.Add(high.Sub(low) / 2).Truncate(time.Second),
				high.Add(-time.Second), high, high.Add(time.Hour),
			} {
				if wall.Before(minDate) || wall.After(maxDate) {
					continue
				}
				value := wall.Format("20060102T150405")
				want, _ := host.resolveInstant(tzid, value, icalFloatingDateTime)
				offset, ok := observanceOffsetAtWall(observances, wall)
				if got := wall.Add(-offset); !ok || !got.Equal(want) {
					t.Errorf("%s: %s placed at %s (ok %v), host places it at %s", tzid, value, got, ok, want)
				}
			}
		}
	}
}

// An open-ended series is expanded instance by instance out to
// CALDAV:max-date-time, and the stored object places every instance where the
// object as submitted placed it.
func TestStoredDefinitionPlacesEveryInstanceOfAnOpenEndedSeries(t *testing.T) {
	for _, tzid := range fidelityZones {
		t.Run(tzid, func(t *testing.T) {
			requireHostZones(t, tzid)
			raw := producerObject("VEVENT", "UID:e", "DTSTAMP:20261002T120000Z",
				"DTSTART;TZID="+tzid+":20260105T103000", "DTEND;TZID="+tzid+":20260105T113000", "RRULE:FREQ=WEEKLY;BYDAY=MO,SA")
			validated, fault := validateCalendarObjectForStorage(raw, nil)
			if fault != nil {
				t.Fatalf("refused: %+v", fault)
			}
			if validated.Body == raw {
				t.Fatal("no definition was stored")
			}
			submitted, stored := seriesStarts(t, raw), seriesStarts(t, validated.Body)
			if len(submitted) < 7000 {
				t.Fatalf("expansion reached %d instances, want the series through max-date-time", len(submitted))
			}
			assertSameStarts(t, stored, submitted)
		})
	}
}

// seriesStarts expands the recurring VEVENT of body, its last component, out
// to CALDAV:max-date-time the way the report matcher does.
func seriesStarts(t *testing.T, body string) []time.Time {
	t.Helper()
	_, maxDate := ical.DateLimits()
	root, err := parseICalendarObject(body)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	m := newCalendarTimeRangeMatcher(body, root, floatingZone{})
	master := root.children[len(root.children)-1]
	dtstart, ok := m.dateValue(master, "DTSTART", noShift)
	if !ok {
		t.Fatal("DTSTART does not resolve")
	}
	var starts []time.Time
	err = ical.VisitRecurrenceInstances(body, "VEVENT", dtstart.instant, time.Hour, dtstart.instant, maxDate, 1<<20,
		m, func(instance ical.RecurrenceInstance) bool {
			starts = append(starts, instance.Start)
			return false
		})
	if err != nil {
		t.Fatalf("expansion: %v", err)
	}
	return starts
}

func assertSameStarts(t *testing.T, got, want []time.Time) {
	t.Helper()
	for i := range min(len(got), len(want)) {
		if !got[i].Equal(want[i]) {
			t.Fatalf("instance %d placed at %s, want %s", i, got[i], want[i])
		}
	}
	if len(got) != len(want) {
		t.Fatalf("%d instances, want %d", len(got), len(want))
	}
}
