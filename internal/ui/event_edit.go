package ui

import (
	"slices"
	"strconv"
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

// editorWrittenProperties are the properties an edit writes from the editor's
// fields, and those what it writes supersedes: DURATION, by the DTEND it
// writes. An attachment the editor cannot show is not written by it; see
// editorWritten.
var editorWrittenProperties = map[string]struct{}{
	"UID": {}, "DTSTAMP": {}, "DTSTART": {}, "DTEND": {}, "DURATION": {}, "RECURRENCE-ID": {},
	"SUMMARY": {}, "LOCATION": {}, "DESCRIPTION": {}, "URL": {}, "STATUS": {}, "CLASS": {},
	"TRANSP": {}, "CATEGORIES": {}, "ORGANIZER": {}, "ATTENDEE": {}, "ATTACH": {}, "RRULE": {},
}

// editorWritten reports whether an edit writes the property named name, whose
// content line has keyPart, from the editor's fields.
func editorWritten(name, keyPart string) bool {
	if _, written := editorWrittenProperties[name]; !written {
		return false
	}
	return name != "ATTACH" || !binaryAttachment(keyPart)
}

// binaryAttachment reports whether an ATTACH with keyPart carries the
// attachment inline (RFC 5545 §3.8.1.1), which the editor, whose field holds
// URIs, does not show.
func binaryAttachment(keyPart string) bool {
	return ical.PropertyParamEquals(keyPart, "VALUE", "BINARY") || ical.PropertyParamEquals(keyPart, "ENCODING", "BASE64")
}

// withUneditedProperties returns edited, the master VEVENT a series edit built
// from the editor's fields, merged with previous, the master it replaces, as
// mergeEdit merges them; both are the content lines between BEGIN:VEVENT and
// END:VEVENT. object is the unfolded calendar object the edit is stored as,
// whose zones the series' times are read in, and recurrence the repeat
// fields the editor submitted, whose RRULE keeps what keptRule keeps of
// previous'.
//
// EXDATE, RDATE and EXRULE add to or take from the series' instances: they are
// dropped when the edit ends the series, and when it moves the series' start
// to another time of day, zone or value type, each EXDATE and RDATE date is
// restated to name the same day's instance of the moved series.
func withUneditedProperties(edited, previous, object []string, form *utils.EventOptions, recurrence *utils.RecurrenceOptions) []string {
	properties, _ := splitComponentBody(edited)
	previousProperties, _ := splitComponentBody(previous)
	endsSeries := firstPropertyLine(previousProperties, "RRULE") != "" && firstPropertyLine(properties, "RRULE") == ""
	move, moved := newSeriesMove(previousProperties, properties, object)
	if shown, _, ok := shownRecurrence(previousProperties, object); ok {
		rule := firstPropertyLine(properties, "RRULE")
		if i := slices.Index(edited, rule); rule != "" && i >= 0 {
			edited = slices.Clone(edited)
			edited[i] = keptRule(rule, firstPropertyLine(previousProperties, "RRULE"), recurrence, shown, moved)
		}
	}
	return mergeEdit(edited, previous, form, func(name, line string) (string, bool) {
		switch name {
		case "EXDATE", "RDATE", "EXRULE":
			if endsSeries {
				return "", false
			}
			if moved {
				return move.restate(line), true
			}
		}
		return line, true
	})
}

// withUneditedOverrideProperties returns edited, the override an occurrence
// edit built from the editor's fields, merged with previous as mergeEdit
// merges them. previous is the override the edit replaces, or, for an
// occurrence edited for the first time, the series' master, whose data the
// instance had: a client copies the master into a new override the same way.
// What makes a master a series is not an override's, and is left out. An
// override of this and every later instance stays one: the RANGE of previous'
// RECURRENCE-ID is kept.
func withUneditedOverrideProperties(edited, previous []string, form *utils.EventOptions) []string {
	if previousID := firstPropertyLine(previous, "RECURRENCE-ID"); previousID != "" {
		keyPart, _, _ := ical.SplitContentLine(previousID)
		if r, ok := ical.PropertyParam(keyPart, "RANGE"); ok {
			edited = slices.Clone(edited)
			for i, line := range edited {
				if editedKeyPart, value, ok := ical.SplitContentLine(line); ok && strings.EqualFold(strings.TrimSpace(ical.PropertyName(editedKeyPart)), "RECURRENCE-ID") {
					edited[i] = editedKeyPart + ";RANGE=" + r + ":" + value
					break
				}
			}
		}
	}
	return mergeEdit(edited, previous, form, func(name, line string) (string, bool) {
		switch name {
		case "EXDATE", "RDATE", "EXRULE":
			return "", false
		}
		return line, true
	})
}

// mergeEdit returns edited, a component an edit built from the editor's
// fields, with what the editor did not change kept as previous, the component
// it replaces, holds it; both are the content lines between the component's
// BEGIN and END. form holds the editor's fields.
//
// A field submitted as the editor showed it keeps previous' lines, with the
// parameters the editor has no field for: LANGUAGE, ALTREP, FMTTYPE, and
// an attendee's PARTSTAT, ROLE, RSVP and the like (see keptFields).
// Carried over too are the properties the editor has no field for, the alarms
// it does not show as reminders and those it does whose reminder the edit
// kept (see keptAlarms), X-ALT-DESC while the description it renders is
// unchanged, and LAST-MODIFIED, set to the time of the edit. carry may rewrite
// or drop each other property previous holds, given its name.
func mergeEdit(edited, previous []string, form *utils.EventOptions, carry func(name, line string) (string, bool)) []string {
	properties, components := splitComponentBody(edited)
	previousProperties, previousComponents := splitComponentBody(previous)

	descriptionKept := propertyText(previousProperties, "DESCRIPTION") == propertyText(properties, "DESCRIPTION")
	editedAt := propertyValue(firstPropertyLine(properties, "DTSTAMP"))
	properties = keptFields(properties, previousProperties, form)

	for _, line := range previousProperties {
		keyPart, _, ok := ical.SplitContentLine(line)
		if !ok {
			continue
		}
		name := strings.ToUpper(strings.TrimSpace(ical.PropertyName(keyPart)))
		if editorWritten(name, keyPart) {
			continue
		}
		switch name {
		case "LAST-MODIFIED":
			line = "LAST-MODIFIED:" + editedAt
		case "X-ALT-DESC":
			if !descriptionKept {
				continue
			}
		}
		line, kept := carry(name, line)
		if !kept {
			continue
		}
		properties = append(properties, line)
	}
	for _, component := range keptAlarms(components, previousComponents) {
		properties = append(properties, component...)
	}
	return properties
}

// keptFields returns properties, those an edit built from form, the editor's
// fields, with each field submitted as the editor showed it from previous, the
// properties of the component the edit replaces, written as previous writes
// it. The editor shows a value without its parameters, and a list without
// how previous split it over lines, so what previous writes beside the value
// is kept as long as the value is.
//
// Addresses and attachments are matched one by one: an attendee submitted as
// shown keeps its line, and one whose address is unchanged but whose name the
// edit changed keeps its other parameters with the new name. ORGANIZER,
// ATTENDEE and ATTACH are written after the other properties, in the form's
// order.
func keptFields(properties, previous []string, form *utils.EventOptions) []string {
	kept := make([]string, 0, len(properties))
	for _, line := range properties {
		keyPart, value, ok := ical.SplitContentLine(line)
		if !ok {
			kept = append(kept, line)
			continue
		}
		name := strings.ToUpper(strings.TrimSpace(ical.PropertyName(keyPart)))
		switch name {
		case "SUMMARY", "LOCATION", "DESCRIPTION":
			if previousLine := firstPropertyLine(previous, name); previousLine != "" && propertyText([]string{previousLine}, name) == propertyText([]string{line}, name) {
				line = previousLine
			}
		case "URL":
			if previousLine := firstPropertyLine(previous, name); previousLine != "" && propertyValue(previousLine) == strings.TrimSpace(value) {
				line = previousLine
			}
		case "STATUS", "CLASS", "TRANSP":
			if previousLine := firstPropertyLine(previous, name); previousLine != "" && strings.EqualFold(propertyValue(previousLine), strings.TrimSpace(value)) {
				line = previousLine
			}
		case "CATEGORIES":
			if slices.Equal(form.Categories, submittedCategories(previous)) {
				kept = append(kept, propertyLines(previous, "CATEGORIES")...)
				continue
			}
		case "ORGANIZER", "ATTENDEE", "ATTACH":
			continue
		}
		kept = append(kept, line)
	}
	kept = append(kept, keptAddresses("ORGANIZER", []string{form.Organizer}, previous)...)
	kept = append(kept, keptAddresses("ATTENDEE", form.Attendees, previous)...)
	return append(kept, keptAttachments(form.Attachments, previous)...)
}

// submittedCategories is what the editor submits for the categories of a
// component with properties when they are left as it shows them: every
// category, joined into one field and split again at its commas.
func submittedCategories(properties []string) []string {
	var categories []string
	for _, line := range propertyLines(properties, "CATEGORIES") {
		categories = append(categories, splitICalTextList(propertyValue(line))...)
	}
	return splitListField(strings.Join(categories, ","))
}

// keptAddresses returns the content lines of prop, ORGANIZER or ATTENDEE, for
// the addresses the form submits: previous' line for an address submitted as
// the editor shows that line, previous' line with the submitted name for one
// whose address is unchanged, and a new line otherwise. Only a mail address
// can be new; previous may also name a room or resource by another URI.
func keptAddresses(prop string, submitted, previous []string) []string {
	candidates := propertyLines(previous, prop)
	used := make([]bool, len(candidates))
	byShown := make(map[string][]int, len(candidates))
	byAddress := make(map[string][]int, len(candidates))
	for i, line := range candidates {
		byShown[shownAddress(line)] = append(byShown[shownAddress(line)], i)
		if address := normalizedAddress(propertyValue(line)); address != "" {
			byAddress[address] = append(byAddress[address], i)
		}
	}
	take := func(index map[string][]int, key string) string {
		for len(index[key]) > 0 {
			i := index[key][0]
			index[key] = index[key][1:]
			if !used[i] {
				used[i] = true
				return candidates[i]
			}
		}
		return ""
	}
	var lines []string
	for _, entry := range submitted {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		line := take(byShown, entry)
		if line == "" {
			name, address := utils.NameAndAddress(entry)
			if previousLine := take(byAddress, normalizedAddress(address)); previousLine != "" {
				line = withDisplayName(previousLine, utils.DisplayNameParam(name))
			} else {
				line = utils.AttendeeLine(prop, entry)
			}
		}
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

// shownAddress is the address the editor shows for an ORGANIZER or ATTENDEE
// line.
func shownAddress(line string) string {
	_, params, value, _ := parseICalProperty(line)
	return formatICalEmailValue(value, params)
}

// normalizedAddress is a calendar user address with the mailto scheme and
// case set aside, as two addresses naming the same user compare.
func normalizedAddress(address string) string {
	address = strings.TrimSpace(address)
	if len(address) >= len("mailto:") && strings.EqualFold(address[:len("mailto:")], "mailto:") {
		address = strings.TrimSpace(address[len("mailto:"):])
	}
	return strings.ToLower(address)
}

// withDisplayName returns line, an ORGANIZER or ATTENDEE line, with name, a
// CN parameter value as utils.DisplayNameParam writes it, in place of its own
// CN, or with no CN when name is "".
func withDisplayName(line, name string) string {
	keyPart, value, _ := ical.SplitContentLine(line)
	parts := splitICalParameterParts(keyPart)
	rewritten := []string{parts[0]}
	for _, part := range parts[1:] {
		if parameter, _, _ := strings.Cut(part, "="); !strings.EqualFold(strings.TrimSpace(parameter), "CN") {
			rewritten = append(rewritten, part)
		}
	}
	if name != "" {
		rewritten = append(rewritten, "CN="+name)
	}
	return strings.Join(rewritten, ";") + ":" + value
}

// keptAttachments returns the ATTACH lines for the URIs the form submits:
// previous' line for one previous holds, with its FMTTYPE, and a new line
// otherwise. An inline attachment is not the editor's, and mergeEdit carries
// it over as previous holds it.
func keptAttachments(submitted, previous []string) []string {
	var candidates []string
	for _, line := range propertyLines(previous, "ATTACH") {
		if keyPart, _, _ := ical.SplitContentLine(line); !binaryAttachment(keyPart) {
			candidates = append(candidates, line)
		}
	}
	var lines []string
	for _, uri := range submitted {
		uri = strings.TrimSpace(uri)
		if uri == "" {
			continue
		}
		var line string
		if i := slices.IndexFunc(candidates, func(line string) bool { return propertyValue(line) == uri }); i >= 0 {
			line = candidates[i]
			candidates = slices.Delete(candidates, i, i+1)
		} else {
			line = utils.AttachmentLine(uri)
		}
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

// keptAlarms returns the alarms an edited component holds: edited, the
// reminders the editor submitted, built as alarms, and previous, the alarms of
// the component it replaces. Each alarm of previous the editor does not show
// as a reminder is kept, and so is each it does whose reminder was submitted,
// as written -- with an RFC 9074 UID and ACKNOWLEDGED, so a dismissed alarm
// stays dismissed -- in previous' order. A submitted reminder that no alarm of
// previous shows follows them.
func keptAlarms(edited, previous [][]string) [][]string {
	submitted := make(map[int]int)
	for _, alarm := range edited {
		if minutes, ok := editorReminder(alarm); ok {
			submitted[minutes]++
		}
	}
	var kept [][]string
	for _, alarm := range previous {
		if minutes, shown := editorReminder(alarm); shown {
			if submitted[minutes] == 0 {
				continue
			}
			submitted[minutes]--
		}
		kept = append(kept, alarm)
	}
	for _, alarm := range edited {
		if minutes, ok := editorReminder(alarm); ok && submitted[minutes] > 0 {
			submitted[minutes]--
			kept = append(kept, alarm)
		}
	}
	return kept
}

// editorReminder is how many minutes before its event's start a component, its
// lines from BEGIN through END, reminds of it, when the editor shows it as a
// reminder: a VALARM whose TRIGGER is a duration related to the start, at or
// before it, whatever its ACTION. An edit keeps such an alarm as written
// while its reminder is submitted (see keptAlarms), and a reminder the editor
// adds is a DISPLAY alarm. An alarm at a date-time, one related to the end,
// or one after the start cannot be shown as minutes before the start, and an
// edit keeps it as written.
func editorReminder(component []string) (int, bool) {
	if len(component) == 0 || !strings.EqualFold(strings.TrimSpace(component[0]), "BEGIN:VALARM") {
		return 0, false
	}
	var trigger string
	var triggerParams map[string]string
	depth := 0
	for _, line := range component[1:] {
		key, params, value, ok := parseICalProperty(line)
		if !ok {
			continue
		}
		switch {
		case key == "BEGIN":
			depth++
		case key == "END":
			depth--
		case depth != 0:
		case key == "TRIGGER" && trigger == "":
			trigger, triggerParams = value, params
		}
	}
	if trigger == "" ||
		strings.EqualFold(triggerParams["VALUE"], "DATE-TIME") || strings.EqualFold(triggerParams["RELATED"], "END") {
		return 0, false
	}
	return parseTriggerMinutes(trigger)
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

// propertyLines are the lines of properties of the property named name.
func propertyLines(properties []string, name string) []string {
	var lines []string
	for _, line := range properties {
		if keyPart, _, ok := ical.SplitContentLine(line); ok && strings.EqualFold(strings.TrimSpace(ical.PropertyName(keyPart)), name) {
			lines = append(lines, line)
		}
	}
	return lines
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

// seriesStart is the form of a series' DTSTART, or of its DTEND.
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
	return readSeriesTime(properties, "DTSTART")
}

// readSeriesTime reads the form of the property named name, DTSTART or DTEND,
// among properties.
func readSeriesTime(properties []string, name string) (seriesStart, bool) {
	line := firstPropertyLine(properties, name)
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

// restate rewrites an EXDATE, RDATE or RECURRENCE-ID line so that each date
// it names, an instance of the series as it was, names the instance the moved
// series has on the same day: the wall clock it had in the series' former
// zone, shifted by the change in the series' time of day, in the series' new
// form. A RECURRENCE-ID keeps its RANGE. An RDATE naming periods states their
// own starts and ends rather than instances of the series, as does an EXRULE,
// and is kept as written, as is a line holding a value it cannot read.
func (m seriesMove) restate(line string) string {
	keyPart, value, ok := ical.SplitContentLine(line)
	if !ok || ical.PropertyParamEquals(keyPart, "VALUE", "PERIOD") || strings.Contains(value, "/") {
		return line
	}
	name := strings.ToUpper(strings.TrimSpace(ical.PropertyName(keyPart)))
	parameters := m.to.parameters
	switch name {
	case "EXDATE", "RDATE":
	case "RECURRENCE-ID":
		if r, ok := ical.PropertyParam(keyPart, "RANGE"); ok {
			parameters += ";RANGE=" + r
		}
	default:
		return line
	}
	values := strings.Split(value, ",")
	restated := make([]string, 0, len(values))
	for _, value := range values {
		wall, ok := m.formerWallClock(keyPart, strings.TrimSpace(value))
		if !ok {
			return line
		}
		restated = append(restated, m.to.format(m.shift(wall)))
	}
	return name + parameters + ":" + strings.Join(restated, ",")
}

// shift is the wall clock, or date's midnight, of the moved series' instance on
// the day of former, an instance of the series as it was.
func (m seriesMove) shift(former time.Time) time.Time {
	return former.Add(m.to.timeOfDay() - m.from.timeOfDay())
}

// overridesFollowingSeries returns components, those of a series an edit
// stores, with the overrides among them following the edit of the series'
// master from previous to master, each the content lines between BEGIN:VEVENT
// and END:VEVENT. An override names the instance it replaces by that
// instance's start, so when the edit moves the series' instances to another
// time of day, zone or value type, each override is moved to replace the same
// day's instance of the moved series (see seriesMove.overrideOf), and when the
// edit ends the series, no instance is left to replace and its overrides are
// dropped. object is the unfolded calendar object the edit is stored as.
func overridesFollowingSeries(components [][]string, previous, master, object []string) [][]string {
	previousProperties, _ := splitComponentBody(previous)
	properties, _ := splitComponentBody(master)
	recurs := func(properties []string) bool {
		return firstPropertyLine(properties, "RRULE") != "" || firstPropertyLine(properties, "RDATE") != ""
	}
	endsSeries := recurs(previousProperties) && !recurs(properties)
	move, moved := newSeriesMove(previousProperties, properties, object)
	if !endsSeries && !moved {
		return components
	}
	following := make([][]string, 0, len(components))
	for _, component := range components {
		if utils.RecurrenceIDValue(component) != "" {
			if endsSeries {
				continue
			}
			component = move.overrideOf(component, previousProperties, properties)
		}
		following = append(following, component)
	}
	return following
}

// overrideOf returns override, an override of the series as it was, as an
// override of the moved series: its RECURRENCE-ID names the same day's
// instance of the moved series. An override that left its instance's start
// where it was moves with that instance: its start becomes the moved
// instance's, and its end keeps its length. When the move turns the series
// from timed to all-day or back, an end of one value type cannot be restated
// in the other, so the override's end is written afresh: with the length of
// master, the moved series' master, if the override was as long as previous,
// the master it replaced, and otherwise with its own length, in whole days
// when it becomes all-day.
func (m seriesMove) overrideOf(override, previous, master []string) []string {
	properties, components := splitComponentBody(override)
	idKeyPart, idValue, ok := ical.SplitContentLine(firstPropertyLine(properties, "RECURRENCE-ID"))
	if !ok {
		return override
	}
	instance, ok := m.formerWallClock(idKeyPart, strings.TrimSpace(idValue))
	if !ok {
		return override
	}
	startKeyPart, startValue, _ := ical.SplitContentLine(firstPropertyLine(properties, "DTSTART"))
	startValue = strings.TrimSpace(startValue)
	start, startRead := m.formerWallClock(startKeyPart, startValue)
	startDate := ical.PropertyParamEquals(startKeyPart, "VALUE", "DATE") || len(startValue) == len("20060102")
	followsInstance := startRead && start.Equal(instance) && startDate == m.from.date
	retyped := m.from.date != m.to.date

	moved := make([]string, 0, len(override)+1)
	for _, line := range properties {
		keyPart, value, ok := ical.SplitContentLine(line)
		if !ok {
			moved = append(moved, line)
			continue
		}
		switch strings.ToUpper(strings.TrimSpace(ical.PropertyName(keyPart))) {
		case "RECURRENCE-ID":
			line = m.restate(line)
		case "DTSTART":
			if followsInstance {
				line = "DTSTART" + m.to.parameters + ":" + m.to.format(m.shift(instance))
			}
		case "DTEND":
			if followsInstance {
				if retyped {
					continue
				}
				if end, ok := m.formerWallClock(keyPart, strings.TrimSpace(value)); ok {
					line = "DTEND" + m.to.parameters + ":" + m.to.format(m.shift(end))
				}
			}
		case "DURATION":
			if followsInstance && retyped {
				continue
			}
		}
		moved = append(moved, line)
	}
	if followsInstance && retyped {
		if length, ok := m.retypedLength(properties, start, previous, master); ok {
			moved = append(moved, "DTEND"+m.to.parameters+":"+m.to.format(m.shift(instance).Add(length)))
		}
	}
	for _, component := range components {
		moved = append(moved, component...)
	}
	return moved
}

// retypedLength is the length an override with properties, starting at start
// on the former series' wall clock, has once the series is turned from timed
// to all-day or back, as overrideOf describes.
func (m seriesMove) retypedLength(properties []string, start time.Time, previous, master []string) (time.Duration, bool) {
	var own time.Duration
	if keyPart, value, ok := ical.SplitContentLine(firstPropertyLine(properties, "DTEND")); ok {
		end, ok := m.formerWallClock(keyPart, strings.TrimSpace(value))
		if !ok {
			return seriesLength(master)
		}
		own = end.Sub(start)
	} else if duration, ok := ical.ParseDuration(propertyValue(firstPropertyLine(properties, "DURATION"))); ok {
		own = duration
	} else {
		return seriesLength(master)
	}
	if former, ok := seriesLength(previous); ok && own == former {
		return seriesLength(master)
	}
	if m.to.date {
		const day = 24 * time.Hour
		return max(day, (own+day-1)/day*day), true
	}
	return own, true
}

// seriesLength is how long each instance of a series with master's properties
// lasts in wall-clock time: from its DTSTART to its DTEND, or its DURATION.
func seriesLength(master []string) (time.Duration, bool) {
	if firstPropertyLine(master, "DTEND") == "" {
		return ical.ParseDuration(propertyValue(firstPropertyLine(master, "DURATION")))
	}
	start, ok := readSeriesTime(master, "DTSTART")
	if !ok {
		return 0, false
	}
	end, ok := readSeriesTime(master, "DTEND")
	if !ok || end.date != start.date {
		return 0, false
	}
	return end.wall.Sub(start.wall), true
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

// shownRecurrence is how the editor's repeat fields show the RRULE of a
// series' master with properties, the content lines between its BEGIN and
// END, in object, the unfolded calendar object holding it: its FREQ upper-cased,
// INTERVAL, COUNT, the day its UNTIL ends the series on in the series' zone,
// the weekday of each BYDAY entry, BYMONTHDAY, and a BYMONTH naming one month.
// partial is true when the rule holds more than that: another part, such as
// BYSETPOS, an ordinal BYDAY, several months, or a value the fields cannot
// hold, or a part the fields hide at the rule's frequency: the weekdays,
// shown for a weekly rule, the days of the month, for a monthly or yearly
// one, and the month, for a yearly one. A WKST counts only where it decides
// which instances there are (RFC 5545 §3.3.10): in a weekly rule naming
// weekdays and repeating less than every week. Clients write one on most
// weekly rules. An edit leaving the fields as shown keeps the rule as written
// (see keptRule). ok is false when the master does not repeat by a rule.
func shownRecurrence(properties, object []string) (shown utils.RecurrenceOptions, partial, ok bool) {
	line := firstPropertyLine(properties, "RRULE")
	if line == "" {
		return utils.RecurrenceOptions{}, false, false
	}
	shown.Interval = 1
	_, value, _ := ical.SplitContentLine(line)
	seen := make(map[string]bool)
	weekStart := false
	for _, part := range ruleParts(value) {
		if part.name == "" || seen[part.name] {
			partial = true
			continue
		}
		seen[part.name] = true
		switch part.name {
		case "FREQ":
			shown.Frequency = strings.ToUpper(part.value)
		case "INTERVAL":
			if interval, err := strconv.Atoi(part.value); err == nil && interval > 0 {
				shown.Interval = interval
			} else {
				partial = true
			}
		case "COUNT":
			if count, err := strconv.Atoi(part.value); err == nil && count > 0 {
				shown.Count = count
			} else {
				partial = true
			}
		case "UNTIL":
			if day, ok := untilDay(part.value, properties, object); ok {
				shown.Until = day
			} else {
				partial = true
			}
		case "BYDAY":
			for _, entry := range strings.Split(part.value, ",") {
				weekday, ordinal := byDayWeekday(entry)
				if weekday == "" || ordinal {
					partial = true
				}
				if weekday != "" && !slices.Contains(shown.ByDay, weekday) {
					shown.ByDay = append(shown.ByDay, weekday)
				}
			}
		case "BYMONTHDAY":
			days, err := utils.ParseMonthDays(part.value)
			if err != nil {
				partial = true
			}
			shown.ByMonthDay = days
		case "WKST":
			weekStart = true
		case "BYMONTH":
			if month, err := strconv.Atoi(part.value); err == nil && month >= 1 && month <= 12 {
				shown.ByMonth = month
			} else {
				partial = true
			}
		default:
			partial = true
		}
	}
	if shown.Count > 0 && shown.Until != "" {
		shown.Until, partial = "", true
	}
	if weekStart && shown.Frequency == "WEEKLY" && shown.Interval > 1 && len(shown.ByDay) > 0 {
		partial = true
	}
	monthly := shown.Frequency == "MONTHLY" || shown.Frequency == "YEARLY"
	if len(shown.ByDay) > 0 && shown.Frequency != "WEEKLY" || len(shown.ByMonthDay) > 0 && !monthly ||
		shown.ByMonth != 0 && shown.Frequency != "YEARLY" {
		partial = true
	}
	return shown, partial, true
}

// rulePart is one part of an RRULE value: its name, upper-cased, or "" for a
// part with no "=", and its value, both as ical.RRuleParam reads them, and the
// part as written.
type rulePart struct {
	name, value, text string
}

func ruleParts(value string) []rulePart {
	var parts []rulePart
	for text := range strings.SplitSeq(value, ";") {
		part := rulePart{text: text}
		if name, value, ok := strings.Cut(text, "="); ok {
			part.name, part.value = strings.ToUpper(strings.TrimSpace(name)), strings.TrimSpace(value)
		}
		parts = append(parts, part)
	}
	return parts
}

// byDayWeekday is the weekday a BYDAY entry names, upper-cased, and whether
// the entry restricts it to an ordinal one of its period, such as 2MO or -1FR.
// weekday is "" for an entry naming none.
func byDayWeekday(entry string) (weekday string, ordinal bool) {
	entry = strings.ToUpper(strings.TrimSpace(entry))
	if len(entry) < 2 {
		return "", false
	}
	weekday = entry[len(entry)-2:]
	switch weekday {
	case "MO", "TU", "WE", "TH", "FR", "SA", "SU":
	default:
		return "", false
	}
	if prefix := entry[:len(entry)-2]; prefix != "" {
		if n, err := strconv.Atoi(prefix); err != nil || n == 0 || n < -53 || n > 53 {
			return "", false
		}
		ordinal = true
	}
	return weekday, ordinal
}

// untilDay is the day, as YYYY-MM-DD, an UNTIL value ends the series of a
// master with properties on: the date a DATE or a floating value names, and
// the date a UTC value has in the series' zone, read in object as the editor
// writes it (utils.EventZoneIn).
func untilDay(value string, properties, object []string) (string, bool) {
	value = strings.TrimSpace(value)
	if len(value) == len("20060102") {
		day, err := time.Parse("20060102", value)
		return day.Format("2006-01-02"), err == nil
	}
	wall, err := ical.ParseDateTime(value)
	if err != nil {
		return "", false
	}
	if strings.HasSuffix(strings.ToUpper(value), "Z") {
		start, _ := readSeriesStart(properties)
		wall = utils.EventZoneIn(start.tzid, object).WallClock(wall)
	}
	return wall.Format("2006-01-02"), true
}

// keptRule returns the RRULE line a series edit writes: edited, the line the
// editor's fields build from submitted, merged with previous, the line the
// series' master holds, which the fields showed as shown. An edit that keeps
// the series' frequency keeps every part of previous the fields do not show,
// such as BYSETPOS, WKST or an ordinal BYDAY, and each part whose field it
// left as shown, as written. A field it changed writes its part as edited
// does, except BYDAY: a weekday left checked keeps its entries, ordinal ones
// included, and one newly checked is added plain. An UNTIL whose fields were
// left as shown is restated from its day when the edit moves the series to
// another time of day, zone or value type (moved), since it named an instant
// of the series as it was. An edit changing the frequency, or ending the
// series, writes edited, and so does one whose merge is a rule the server
// cannot expand, such as a BYSETPOS left with no part to select from.
func keptRule(edited, previous string, submitted *utils.RecurrenceOptions, shown utils.RecurrenceOptions, moved bool) string {
	if edited == "" || previous == "" || submitted == nil || !strings.EqualFold(submitted.Frequency, shown.Frequency) {
		return edited
	}
	_, previousValue, _ := ical.SplitContentLine(previous)
	_, editedValue, _ := ical.SplitContentLine(edited)
	parts := ruleParts(previousValue)
	written := ical.RRuleParts(editedValue)
	changed := false
	set := func(name, value string) {
		changed = true
		kept := parts[:0:0]
		placed := false
		for _, part := range parts {
			if part.name != name {
				kept = append(kept, part)
			} else if value != "" && !placed {
				kept = append(kept, rulePart{name: name, value: value, text: name + "=" + value})
				placed = true
			}
		}
		if value != "" && !placed {
			kept = append(kept, rulePart{name: name, value: value, text: name + "=" + value})
		}
		parts = kept
	}
	if submitted.Interval != shown.Interval {
		set("INTERVAL", written["INTERVAL"])
	}
	if submitted.Count != shown.Count || submitted.Until != shown.Until {
		set("COUNT", written["COUNT"])
		set("UNTIL", written["UNTIL"])
	} else if moved && written["UNTIL"] != "" && slices.ContainsFunc(parts, func(part rulePart) bool { return part.name == "UNTIL" }) {
		set("UNTIL", written["UNTIL"])
	}
	if !sameWeekdays(submitted.ByDay, shown.ByDay) {
		var byDay string
		for _, part := range parts {
			if part.name == "BYDAY" {
				byDay = part.value
				break
			}
		}
		set("BYDAY", keptByDay(byDay, submitted.ByDay))
	}
	if !slices.Equal(submitted.ByMonthDay, shown.ByMonthDay) {
		set("BYMONTHDAY", written["BYMONTHDAY"])
	}
	if submitted.ByMonth != shown.ByMonth {
		set("BYMONTH", written["BYMONTH"])
	}
	if !changed {
		return previous
	}
	texts := make([]string, len(parts))
	for i, part := range parts {
		texts[i] = part.text
	}
	merged := strings.Join(texts, ";")
	if !ical.ValidRecurrenceRule(merged) {
		return edited
	}
	return "RRULE:" + merged
}

// sameWeekdays reports whether two lists of weekdays name the same ones.
func sameWeekdays(a, b []string) bool {
	return !slices.ContainsFunc(a, func(day string) bool { return !slices.Contains(b, day) }) &&
		!slices.ContainsFunc(b, func(day string) bool { return !slices.Contains(a, day) })
}

// keptByDay is the BYDAY value for the weekdays checked, given previous, the
// BYDAY value the master held: each entry of previous on a checked weekday, as
// written, then each checked weekday no entry names, plain.
func keptByDay(previous string, checked []string) string {
	var entries []string
	named := make(map[string]bool)
	if previous != "" {
		for _, entry := range strings.Split(previous, ",") {
			if weekday, _ := byDayWeekday(entry); weekday != "" && slices.Contains(checked, weekday) {
				entries = append(entries, strings.TrimSpace(entry))
				named[weekday] = true
			}
		}
	}
	for _, weekday := range checked {
		if !named[weekday] {
			entries = append(entries, weekday)
			named[weekday] = true
		}
	}
	return strings.Join(entries, ",")
}
