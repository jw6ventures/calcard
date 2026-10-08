package ui

import (
	"strings"
	"time"

	"github.com/jw6ventures/calcard/internal/ical"
	"github.com/jw6ventures/calcard/internal/ui/utils"
)

// withoutServerZoneDefinitions returns lines, the unfolded content lines of a
// calendar object, less each VTIMEZONE beneath its VCALENDAR that the server
// wrote for a zone the host defines. Such a definition covers only the years it
// was generated for, and an edit may move times outside them, so the edit
// reads the zone as the host does and the validator stores a fresh definition.
// Every other definition is the object's own, which RFC 4791 §4.1 makes
// authoritative, and is kept.
func withoutServerZoneDefinitions(lines []string) []string {
	var kept []string
	keptThrough := 0
	for _, timezone := range ical.TimezoneComponents(lines) {
		if !serverZoneDefinition(timezone) {
			continue
		}
		kept = append(kept, lines[keptThrough:timezone.Start]...)
		keptThrough = timezone.End
	}
	if keptThrough == 0 {
		return lines
	}
	return append(kept, lines[keptThrough:]...)
}

// serverZoneDefinition reports whether timezone is one the server wrote for a
// zone the host defines.
func serverZoneDefinition(timezone ical.TimezoneComponent) bool {
	if !timezone.HostDefinition {
		return false
	}
	_, known := ical.HostLocation(timezone.TZID)
	return known
}

// editorWrittenProperties are the master properties a series edit writes from
// the editor's fields, and those what it writes supersedes: DURATION, by the
// DTEND it writes.
var editorWrittenProperties = map[string]struct{}{
	"UID": {}, "DTSTAMP": {}, "DTSTART": {}, "DTEND": {}, "DURATION": {}, "RECURRENCE-ID": {},
	"SUMMARY": {}, "LOCATION": {}, "DESCRIPTION": {}, "URL": {}, "STATUS": {}, "CLASS": {},
	"TRANSP": {}, "CATEGORIES": {}, "ORGANIZER": {}, "ATTENDEE": {}, "ATTACH": {}, "RRULE": {},
}

// withUneditedProperties returns edited, the master VEVENT a series edit built
// from the editor's fields, with what the editor does not edit carried over
// from previous, the master it replaces; both are the content lines between
// BEGIN:VEVENT and END:VEVENT. object is the unfolded calendar object the edit
// is stored as, whose zones the series' times are read in.
//
// Carried over are the properties the editor has no field for, the alarms it
// cannot show as a reminder, X-ALT-DESC while the description it renders is
// unchanged, and LAST-MODIFIED, set to the time of the edit. EXDATE, RDATE and
// EXRULE add to or take from the series' instances: they are dropped when the
// edit ends the series, and when it moves the series' start to another time of
// day, zone or value type, each EXDATE and RDATE date is restated to name the
// same day's instance of the moved series.
func withUneditedProperties(edited, previous, object []string) []string {
	properties, components := splitComponentBody(edited)
	previousProperties, previousComponents := splitComponentBody(previous)

	endsSeries := firstPropertyLine(previousProperties, "RRULE") != "" && firstPropertyLine(properties, "RRULE") == ""
	move, moved := newSeriesMove(previousProperties, properties, object)
	descriptionKept := propertyText(previousProperties, "DESCRIPTION") == propertyText(properties, "DESCRIPTION")
	editedAt := propertyValue(firstPropertyLine(properties, "DTSTAMP"))

	for _, line := range previousProperties {
		keyPart, _, ok := ical.SplitContentLine(line)
		if !ok {
			continue
		}
		name := strings.ToUpper(strings.TrimSpace(ical.PropertyName(keyPart)))
		if _, written := editorWrittenProperties[name]; written {
			continue
		}
		switch name {
		case "LAST-MODIFIED":
			line = "LAST-MODIFIED:" + editedAt
		case "X-ALT-DESC":
			if !descriptionKept {
				continue
			}
		case "EXDATE", "RDATE", "EXRULE":
			if endsSeries {
				continue
			}
			if moved {
				line = move.restate(line)
			}
		}
		properties = append(properties, line)
	}
	for _, component := range previousComponents {
		if !editorShownAlarm(component) {
			components = append(components, component)
		}
	}
	for _, component := range components {
		properties = append(properties, component...)
	}
	return properties
}

// splitComponentBody splits a component's content lines between its BEGIN and
// END into its own properties and its nested components, each from its BEGIN
// line through its END line. The nesting is read as ical.ReadComponents reads
// a stored object, so the edit sees the components every other reader does: a
// component left open is closed where its parent ends, and an END naming no
// open component is left out. Each nested component is written back balanced,
// its own properties ahead of its components, as RFC 5545 §3.6 orders them.
func splitComponentBody(lines []string) (properties []string, components [][]string) {
	type component struct {
		name     string
		lines    []string
		children []*component
	}
	root, body := &component{}, &component{}
	wrapped := make([]string, 0, len(lines)+4)
	wrapped = append(wrapped, "BEGIN:VCALENDAR", "BEGIN:X-CALCARD-BODY")
	wrapped = append(wrapped, lines...)
	wrapped = append(wrapped, "END:X-CALCARD-BODY", "END:VCALENDAR")
	_, _ = ical.ReadComponents(wrapped, false,
		func(parent *component, name string) (*component, error) {
			switch parent {
			case nil:
				return root, nil
			case root:
				return body, nil
			}
			child := &component{name: name}
			parent.children = append(parent.children, child)
			return child, nil
		},
		func(node *component, line string) error {
			node.lines = append(node.lines, line)
			return nil
		})
	var written func(node *component) []string
	written = func(node *component) []string {
		out := append([]string{"BEGIN:" + node.name}, node.lines...)
		for _, child := range node.children {
			out = append(out, written(child)...)
		}
		return append(out, "END:"+node.name)
	}
	for _, child := range body.children {
		components = append(components, written(child))
	}
	return body.lines, components
}

func firstPropertyLine(properties []string, name string) string {
	for _, line := range properties {
		if keyPart, _, ok := ical.SplitContentLine(line); ok && strings.EqualFold(strings.TrimSpace(ical.PropertyName(keyPart)), name) {
			return line
		}
	}
	return ""
}

func propertyValue(line string) string {
	_, value, _ := ical.SplitContentLine(line)
	return strings.TrimSpace(value)
}

// propertyText is the text of a component's first property of that name as
// the view model shows it.
func propertyText(properties []string, name string) string {
	return strings.TrimSpace(unescapeICalText(propertyValue(firstPropertyLine(properties, name))))
}

// editorShownAlarm reports whether the view model shows a VALARM, its lines
// from BEGIN through END, as a reminder, which the edit writes back as one.
func editorShownAlarm(component []string) bool {
	if len(component) == 0 || !strings.EqualFold(strings.TrimSpace(component[0]), "BEGIN:VALARM") {
		return false
	}
	for _, line := range component {
		if key, _, value, ok := parseICalProperty(line); ok && key == "TRIGGER" {
			if _, shown := parseTriggerMinutes(value); shown {
				return true
			}
		}
	}
	return false
}

// seriesStart is the form of a series' DTSTART.
type seriesStart struct {
	date bool
	utc  bool
	// tzid is the zone a local DATE-TIME is in, or "" for a floating one.
	tzid string
	// parameters are the DTSTART's parameters, which a value restated in
	// the series' form carries.
	parameters string
	// wall is the start's wall clock, or its date's midnight, held as the
	// UTC time with the same fields.
	wall time.Time
}

func readSeriesStart(properties []string) (seriesStart, bool) {
	line := firstPropertyLine(properties, "DTSTART")
	keyPart, value, ok := ical.SplitContentLine(line)
	if !ok {
		return seriesStart{}, false
	}
	value = strings.TrimSpace(value)
	start := seriesStart{parameters: keyPart[len(ical.PropertyName(keyPart)):]}
	start.date = ical.PropertyParamEquals(keyPart, "VALUE", "DATE") || len(value) == len("20060102")
	var err error
	if start.date {
		start.wall, err = time.Parse("20060102", value)
		return start, err == nil
	}
	start.utc = strings.HasSuffix(strings.ToUpper(value), "Z")
	if !start.utc {
		tzid, _ := ical.PropertyParam(keyPart, "TZID")
		start.tzid = strings.TrimSpace(tzid)
	}
	start.wall, err = ical.ParseDateTime(value)
	return start, err == nil
}

// timeOfDay is how long after its date's midnight the series starts.
func (s seriesStart) timeOfDay() time.Duration {
	if s.date {
		return 0
	}
	return s.wall.Sub(time.Date(s.wall.Year(), s.wall.Month(), s.wall.Day(), 0, 0, 0, 0, time.UTC))
}

func (s seriesStart) format(wall time.Time) string {
	switch {
	case s.date:
		return wall.Format("20060102")
	case s.utc:
		return wall.Format("20060102T150405Z")
	default:
		return wall.Format("20060102T150405")
	}
}

// seriesMove restates the dates of a series whose start moved from one form,
// zone or time of day to another.
type seriesMove struct {
	from, to seriesStart
	// fromZone reads instants on the wall clock of the series' former zone.
	fromZone utils.EventZone
	times    *ical.ObjectTimes
}

// newSeriesMove is the move from the series start previous states to the one
// edited does. moved is false when the edit leaves every instance's date and
// time of day as they were, or when either start cannot be read.
func newSeriesMove(previous, edited, object []string) (move seriesMove, moved bool) {
	from, ok := readSeriesStart(previous)
	if !ok {
		return seriesMove{}, false
	}
	to, ok := readSeriesStart(edited)
	if !ok {
		return seriesMove{}, false
	}
	if from.date == to.date && from.utc == to.utc && from.tzid == to.tzid && from.timeOfDay() == to.timeOfDay() {
		return seriesMove{}, false
	}
	return seriesMove{from: from, to: to, fromZone: utils.EventZoneIn(from.tzid, object), times: ical.NewObjectTimes(object)}, true
}

// restate rewrites an EXDATE or RDATE line so that each date it names, an
// instance of the series as it was, names the instance the moved series has on
// the same day: the wall clock it had in the series' former zone, shifted by
// the change in the series' time of day, in the series' new form. An RDATE
// naming periods states their own starts and ends rather than instances of the
// series, as does an EXRULE, and is kept as written, as is a line holding a
// value it cannot read.
func (m seriesMove) restate(line string) string {
	keyPart, value, ok := ical.SplitContentLine(line)
	if !ok || ical.PropertyParamEquals(keyPart, "VALUE", "PERIOD") || strings.Contains(value, "/") {
		return line
	}
	name := strings.ToUpper(strings.TrimSpace(ical.PropertyName(keyPart)))
	if name != "EXDATE" && name != "RDATE" {
		return line
	}
	values := strings.Split(value, ",")
	restated := make([]string, 0, len(values))
	for _, value := range values {
		wall, ok := m.formerWallClock(keyPart, strings.TrimSpace(value))
		if !ok {
			return line
		}
		restated = append(restated, m.to.format(wall.Add(m.to.timeOfDay()-m.from.timeOfDay())))
	}
	return name + m.to.parameters + ":" + strings.Join(restated, ",")
}

// formerWallClock is the wall clock, or date's midnight, a value of a property
// with this key part names in the series' former zone.
func (m seriesMove) formerWallClock(keyPart, value string) (time.Time, bool) {
	if ical.PropertyParamEquals(keyPart, "VALUE", "DATE") || len(value) == len("20060102") {
		day, err := time.Parse("20060102", value)
		return day, err == nil
	}
	tzid, _ := ical.PropertyParam(keyPart, "TZID")
	if !strings.HasSuffix(strings.ToUpper(value), "Z") && strings.TrimSpace(tzid) == m.from.tzid {
		wall, err := ical.ParseDateTime(value)
		return wall, err == nil
	}
	instant, ok := m.times.ResolvePropertyTime(keyPart, value)
	if !ok {
		return time.Time{}, false
	}
	return m.fromZone.WallClock(instant), true
}
