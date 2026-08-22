package dav

import "strings"

// supportedCalendarDataRequest reports whether a CALDAV:calendar-data in a
// calendaring REPORT names a media type the server can return. RFC 4791 §7.8
// and §7.9 make an unsupported content-type or version a
// CALDAV:supported-calendar-data failure, and §9.6 defaults both attributes to
// the single pair CALDAV:supported-calendar-data advertises. Distinct from the
// §5.3.2.1 precondition of the same name, which governs stored data.
func supportedCalendarDataRequest(calData *calendarDataEl) bool {
	if calData == nil {
		return true
	}
	if calData.ContentType != nil && !strings.EqualFold(*calData.ContentType, "text/calendar") {
		return false
	}
	if calData.Version == nil {
		return true
	}
	for _, supported := range calendarDataVersions {
		if *calData.Version == supported {
			return true
		}
	}
	return false
}

// calendarDataProjection is what a REPORT response needs to render
// CALDAV:calendar-data: the §9.6 selection, and the zone RFC 4791 §7.3 makes
// its recurrence arithmetic resolve floating values against. PROPFIND has
// neither and passes the zero value, which projects nothing.
type calendarDataProjection struct {
	selection *calendarDataEl
	zone      floatingZone
}

// requested reports whether the response carries CALDAV:calendar-data at all,
// which is a different question from whether the selection narrows it.
func (p calendarDataProjection) requested() bool {
	return p.selection != nil
}

func reportCalendarData(report reportRequest) *calendarDataEl {
	if report.Prop != nil && report.Prop.CalendarData != nil {
		return report.Prop.CalendarData
	}
	if report.CalendarData != nil {
		return report.CalendarData
	}
	return nil
}

// filterICalendarData applies the RFC 4791 §9.6 selection to one stored
// calendar object resource.
//
// A selection that narrows nothing returns the stored octets untouched: §9.6
// returns the resource in its entirety when the request names no CALDAV:comp,
// and a report that only names CALDAV:calendar-data owes the client exactly
// what was PUT. Octets that do not parse are likewise returned as they stand,
// since a projection cannot be derived from a tree that was never built.
func filterICalendarData(raw string, projection calendarDataProjection) string {
	selection := projection.selection
	if selection == nil || selection.empty() {
		return raw
	}
	root, err := parseICalendarObject(raw)
	if err != nil {
		return raw
	}
	return writeICalendarObject(projectCalendarData(root, raw, selection, projection.zone))
}

// projectCalendarData applies the recurrence transform, then the free-busy
// trim, then the component and property selection. That is not the order the
// §9.6 content model lists the elements in: the first two rewrite the
// recurrence set and the third decides what of it is returned, so selection has
// to run last or it would judge components the transforms have not produced yet.
//
// Every transform reads dates through one matcher, built over the resource as
// it was stored. A transform hands the next one the tree it produced, which is
// not the document any more -- expansion removes the VTIMEZONE a later value
// may still name -- so the zone lookups have to keep answering from the parse.
func projectCalendarData(root *icalNode, raw string, selection *calendarDataEl, zone floatingZone) *icalNode {
	m := newCalendarTimeRangeMatcher(raw, root, zone)
	source := root
	switch {
	case selection.Expand != nil:
		source = expandCalendarData(m, source, *selection.Expand)
	case selection.LimitRecurrenceSet != nil:
		source = limitCalendarRecurrenceSet(m, source, *selection.LimitRecurrenceSet)
	}
	if selection.LimitFreeBusySet != nil {
		source = limitCalendarFreeBusySet(m, source, *selection.LimitFreeBusySet)
	}
	if selection.Comp == nil {
		return source
	}
	// §9.6.5 requires an expanded instance to carry the RECURRENCE-ID naming it,
	// unconditionally. §9.6's permission to return data that is invalid per its
	// media type covers properties the *media type* requires and the request did
	// not select, which is a different set: the identifier only exists because
	// the server expanded, so a client cannot be said to have declined it.
	var mandatory icalNameSet
	if selection.Expand != nil {
		mandatory = recurrenceIDName
	}
	projected := selectComponent(source, calendarDataRootSelection(selection.Comp), mandatory)
	if projected == nil {
		return nil
	}
	return projected
}

// calendarDataRootSelection scopes the request's CALDAV:comp to the VCALENDAR
// root. §9.6.1 nests the selection under a comp naming VCALENDAR, but a client
// naming an inner component alone is asking for that component out of a
// resource that is still one iCalendar object, so the selection is read as
// nested under an implicit VCALENDAR rather than as a root that matches nothing.
func calendarDataRootSelection(comp *calendarComp) *calendarComp {
	if strings.EqualFold(comp.Name, "VCALENDAR") {
		return comp
	}
	return &calendarComp{Name: "VCALENDAR", Comp: []calendarComp{*comp}}
}

// selectComponent returns the copy of node that sel admits: the properties
// under (allprop | prop*) and the sub-components under (allcomp | comp*), per
// RFC 4791 §9.6.1. A bare CALDAV:comp names neither, and matching zero of each
// is what its content model says, so it returns the component alone. Properties
// named in mandatory survive whatever the selection said. nil means sel names
// no component matching node.
func selectComponent(node *icalNode, sel *calendarComp, mandatory icalNameSet) *icalNode {
	if !strings.EqualFold(sel.Name, node.name) {
		return nil
	}
	projected := &icalNode{name: node.name}
	if sel.AllProp {
		projected.properties = append(projected.properties, node.properties...)
	} else {
		for _, property := range node.properties {
			if mandatory.contains(property.name) {
				projected.properties = append(projected.properties, property)
				continue
			}
			if selected, ok := selectProperty(property, sel.Prop); ok {
				projected.properties = append(projected.properties, selected)
			}
		}
	}
	if sel.AllComp {
		projected.children = append(projected.children, node.children...)
		return projected
	}
	for _, child := range node.children {
		for i := range sel.Comp {
			if selectedChild := selectComponent(child, &sel.Comp[i], mandatory); selectedChild != nil {
				projected.children = append(projected.children, selectedChild)
				break
			}
		}
	}
	return projected
}

// selectProperty matches one content line against the CALDAV:prop list, keeping
// every occurrence of a named property rather than the first: two ATTENDEEs are
// two content lines and §9.6.4 names neither of them individually.
func selectProperty(property icalProperty, selected []calendarProp) (icalProperty, bool) {
	for _, want := range selected {
		if !strings.EqualFold(want.Name, property.name) {
			continue
		}
		if want.NoValue {
			property.value = ""
		}
		return property, true
	}
	return icalProperty{}, false
}
