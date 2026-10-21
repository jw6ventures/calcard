package ui

import (
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/jw6ventures/calcard/internal/calobject"
	"github.com/jw6ventures/calcard/internal/ical"
	"github.com/jw6ventures/calcard/internal/store"
	"github.com/jw6ventures/calcard/internal/ui/utils"
)

// editorAddresses are the calendar user addresses of user, who makes an edit.
func editorAddresses(user *store.User) []string {
	if user == nil || strings.TrimSpace(user.PrimaryEmail) == "" {
		return nil
	}
	return []string{user.PrimaryEmail}
}

// occurrenceZone is the zone the RECURRENCE-ID or EXDATE value of an
// occurrence form is a wall clock in, in object, the unfolded calendar object
// the form edits. A floating series' editor marks the occurrence with
// recurrence_floating, whatever zone is typed for the occurrence's own times,
// and the value is then floating. Otherwise it is in recurrence_timezone, which
// names the zone of the series' instances; a blank one is the series' own
// reading (see utils.EventZoneIn). A form that has no recurrence_timezone, as
// a form of a page loaded before the field existed has none, gives the zone of
// its times.
func occurrenceZone(r *http.Request, object []string) utils.EventZone {
	if r.FormValue("recurrence_floating") == "1" {
		if zone := utils.EventZoneIn("", object); zone.Floating() {
			return zone
		}
	}
	name := r.FormValue("timezone")
	if _, named := r.Form["recurrence_timezone"]; named {
		name = r.FormValue("recurrence_timezone")
	}
	return utils.EventZoneIn(name, object)
}

// occurrence is the instance of a series an RECURRENCE-ID or EXDATE value
// names: a day, or an instant, which a floating series reads in UTC and which a
// UTC or zoned value does not name the same instance of.
type occurrence struct {
	date, floating bool
	at             time.Time
}

// readOccurrence reads the value of a RECURRENCE-ID or EXDATE line with this
// key part, resolving the times of the object times belongs to.
func readOccurrence(keyPart, value string, times *ical.ObjectTimes) (occurrence, bool) {
	value = strings.TrimSpace(value)
	if ical.PropertyParamEquals(keyPart, "VALUE", "DATE") || len(value) == len("20060102") {
		day, err := time.Parse("20060102", value)
		return occurrence{date: true, at: day}, err == nil
	}
	at, ok := times.ResolvePropertyTime(keyPart, value)
	tzid, _ := ical.PropertyParam(keyPart, "TZID")
	floating := strings.TrimSpace(tzid) == "" && !strings.HasSuffix(strings.ToUpper(value), "Z")
	return occurrence{floating: floating, at: at}, ok
}

// excludes reports whether master, the content lines of a series' master,
// already excludes the instance target names with an EXDATE, whatever list or
// spelling of the zone states it.
func excludes(master []string, target occurrence, times *ical.ObjectTimes) bool {
	properties, _ := splitComponentBody(master)
	for _, line := range propertyLines(properties, "EXDATE") {
		keyPart, value, _ := ical.SplitContentLine(line)
		if ical.PropertyParamEquals(keyPart, "VALUE", "PERIOD") {
			continue
		}
		for _, one := range strings.Split(value, ",") {
			if excluded, ok := readOccurrence(keyPart, one, times); ok && excluded == target {
				return true
			}
		}
	}
	return false
}

// withoutOccurrence returns components, the VEVENTs of a stored object whose
// unfolded content lines are object and whose text is raw, as they are once the
// instance exdateLine, an EXDATE line, names is removed: excluded from the
// series' master, which is revised when the editor, whose addresses editor
// holds, may revise it, and its
// override dropped. An override of this and every later instance anchored at
// the instance is not dropped but moved to the next instance it governs, since
// the instance is then excluded and the override would stand for itself at its
// slot; with no such instance it has nothing left to govern and is dropped. It
// reports false when the object has no master.
func (h *Handler) withoutOccurrence(raw string, object []string, components [][]string, exdateLine string, editor []string) (remaining [][]string, masterFound bool) {
	times := ical.NewObjectTimes(object, nil, nil)
	keyPart, value, _ := ical.SplitContentLine(exdateLine)
	target, targetRead := readOccurrence(keyPart, value, times)
	identity := []string{strings.Replace(exdateLine, "EXDATE", "RECURRENCE-ID", 1)}
	for _, component := range components {
		if utils.SameRecurrenceID(component, identity) {
			if moved := h.anchoredAtNextInstance(raw, object, component, times); moved != nil {
				remaining = append(remaining, moved)
			}
			continue
		}
		if utils.RecurrenceIDValue(component) == "" && !masterFound {
			masterFound = true
			if !targetRead || !excludes(component, target, times) {
				component = append(slices.Clone(component), exdateLine)
				if mayRevise(component, nil, editor) {
					component = incrementSequence(component)
				}
			}
		}
		remaining = append(remaining, component)
	}
	return remaining, masterFound
}

// instancesNear lists the instances of the series raw holds that fall within
// a day of at, as the calendar views list them.
func (h *Handler) instancesNear(raw string, at time.Time) []calobject.Instance {
	if h.expandEvent == nil {
		return nil
	}
	return h.expandEvent(raw, at.AddDate(0, 0, -1), at.AddDate(0, 0, 1), nil).Instances
}

// anchoredAtNextInstance returns override, an override of this and every later
// instance, anchored at the next instance it governs, with that instance's
// start and end: the override's RECURRENCE-ID, DTSTART and DTEND are restated in
// the forms they have. raw is the stored object whose unfolded lines are
// object. It returns nil for an override that is not of this and every later
// instance, or governs none after its anchor.
func (h *Handler) anchoredAtNextInstance(raw string, object, override []string, times *ical.ObjectTimes) []string {
	properties, components := splitComponentBody(override)
	idLine := firstPropertyLine(properties, "RECURRENCE-ID")
	idKeyPart, idValue, ok := ical.SplitContentLine(idLine)
	if !ok || !ical.PropertyParamEquals(idKeyPart, "RANGE", "THISANDFUTURE") || h.expandEvent == nil {
		return nil
	}
	anchor, ok := times.ResolvePropertyTime(idKeyPart, idValue)
	if !ok {
		return nil
	}
	var next *calobject.Instance
	for _, days := range []int{8, 40, 400, 4000} {
		expansion := h.expandEvent(raw, anchor, anchor.AddDate(0, 0, days), nil)
		for _, instance := range expansion.Instances {
			if instance.Override.Equal(anchor) && instance.RecurrenceID.After(anchor) && (next == nil || instance.RecurrenceID.Before(next.RecurrenceID)) {
				next = &instance
			}
		}
		if next != nil {
			break
		}
	}
	if next == nil {
		return nil
	}
	moved := make([]string, 0, len(override))
	for _, line := range properties {
		keyPart, value, ok := ical.SplitContentLine(line)
		if ok {
			switch strings.ToUpper(strings.TrimSpace(ical.PropertyName(keyPart))) {
			case "RECURRENCE-ID":
				line = keyPart + ":" + formatInstantLike(keyPart, value, next.RecurrenceID, object)
			case "DTSTART":
				line = keyPart + ":" + formatInstantLike(keyPart, value, next.Start, object)
			case "DTEND":
				line = keyPart + ":" + formatInstantLike(keyPart, value, next.End, object)
			}
		}
		moved = append(moved, line)
	}
	for _, component := range components {
		moved = append(moved, component...)
	}
	return moved
}

// formatInstantLike writes instant as a value of the form value has on a line
// with this key part, in object: a DATE, a UTC time, a wall clock in the zone
// the line names, or a floating wall clock.
func formatInstantLike(keyPart, value string, instant time.Time, object []string) string {
	value = strings.TrimSpace(value)
	if ical.PropertyParamEquals(keyPart, "VALUE", "DATE") || len(value) == len("20060102") {
		return instant.UTC().Format("20060102")
	}
	if strings.HasSuffix(strings.ToUpper(value), "Z") {
		return instant.UTC().Format("20060102T150405Z")
	}
	tzid, _ := ical.PropertyParam(keyPart, "TZID")
	return utils.EventZoneIn(tzid, object).WallClock(instant).Format("20060102T150405")
}

// overrideSource is what an occurrence edit builds its override from.
type overrideSource struct {
	// previous is the override the edit replaces, the override of this and
	// every later instance governing the instance, or else the master.
	previous []string
	// master is the series' master.
	master []string
	// instance is the schedule the instance had, when previous is the override
	// governing it; nil when the expansion does not tell.
	instance *schedule
}

// replacesPrevious reports whether edited is the override previous is: an
// override of the same instance.
func (s overrideSource) replacesPrevious(edited []string) bool {
	return firstPropertyLine(s.previous, "RECURRENCE-ID") != "" && utils.SameRecurrenceID(s.previous, edited)
}

// overrideSourceOf is what the override edited, built by an occurrence edit,
// is made from among components, the VEVENTs of the stored object raw whose
// unfolded content lines are object: the override of its instance if there is
// one, else the latest override of this and every later instance anchored at or
// before it, else the master. previous is nil when there is none.
func (h *Handler) overrideSourceOf(raw string, object []string, components [][]string, edited []string) overrideSource {
	var source overrideSource
	times := ical.NewObjectTimes(object, nil, nil)
	target, targetRead := overrideInstance(edited, times)
	var governing []string
	var anchor time.Time
	for _, component := range components {
		switch {
		case utils.RecurrenceIDValue(component) == "":
			if source.master == nil {
				source.master = component
			}
		case utils.SameRecurrenceID(component, edited):
			source.previous = component
		default:
			properties, _ := splitComponentBody(component)
			keyPart, value, _ := ical.SplitContentLine(firstPropertyLine(properties, "RECURRENCE-ID"))
			start, read := readOccurrence(keyPart, value, times)
			if read && targetRead && ical.PropertyParamEquals(keyPart, "RANGE", "THISANDFUTURE") &&
				start.date == target.date && start.floating == target.floating &&
				!start.at.After(target.at) && (governing == nil || start.at.After(anchor)) {
				governing, anchor = component, start.at
			}
		}
	}
	switch {
	case source.previous != nil:
	case governing != nil:
		source.previous = governing
		source.instance = h.instanceSchedule(raw, governing, target, times)
	default:
		source.previous = source.master
	}
	return source
}

// overrideInstance is the instance the RECURRENCE-ID of override, the content
// lines of a component, names.
func overrideInstance(override []string, times *ical.ObjectTimes) (occurrence, bool) {
	properties, _ := splitComponentBody(override)
	keyPart, value, ok := ical.SplitContentLine(firstPropertyLine(properties, "RECURRENCE-ID"))
	if !ok {
		return occurrence{}, false
	}
	return readOccurrence(keyPart, value, times)
}

// instanceSchedule is the schedule the instance target of the series raw holds,
// which override, an override of this and every later instance, governs; nil
// when the expansion does not list the instance near its slot.
func (h *Handler) instanceSchedule(raw string, override []string, target occurrence, times *ical.ObjectTimes) *schedule {
	for _, instance := range h.instancesNear(raw, target.at) {
		if instance.RecurrenceID.Equal(target.at) {
			span, ok := readSchedule(override, times)
			if !ok {
				return nil
			}
			span.start, span.end = instance.Start, instance.End
			return &span
		}
	}
	return nil
}
