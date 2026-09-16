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
	// index is the selection resolved by name. One report applies one selection
	// to every resource it matches, so building it per resource would multiply
	// the request body against the collection.
	index *calendarCompIndex
}

// newCalendarDataProjection resolves the §9.6 selection by name once, for the
// whole report.
func newCalendarDataProjection(selection *calendarDataEl, zone floatingZone) calendarDataProjection {
	projection := calendarDataProjection{selection: selection, zone: zone}
	if selection != nil && selection.Comp != nil {
		projection.index = newCalendarCompIndex(calendarDataRootSelection(selection.Comp))
	}
	return projection
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
func filterICalendarData(raw string, projection calendarDataProjection) (string, error) {
	selection := projection.selection
	if selection == nil || selection.empty() {
		return raw, nil
	}
	root, err := parseICalendarObject(raw)
	if err != nil {
		return raw, nil
	}
	index := projection.index
	if index == nil && selection.Comp != nil {
		// A projection built without newCalendarDataProjection still projects
		// correctly, at the per-resource cost the index exists to avoid.
		index = newCalendarCompIndex(calendarDataRootSelection(selection.Comp))
	}
	projected, err := projectCalendarData(root, raw, selection, index, projection.zone)
	if err != nil {
		return "", err
	}
	return writeICalendarObject(projected), nil
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
func projectCalendarData(root *icalNode, raw string, selection *calendarDataEl, index *calendarCompIndex, zone floatingZone) (*icalNode, error) {
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
	if *m.expansionError != nil {
		return nil, *m.expansionError
	}
	if selection.Comp == nil {
		return source, nil
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
	projected := selectComponent(source, index, mandatory)
	if projected == nil {
		return nil, nil
	}
	return projected, nil
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

// calendarCompIndex is one CALDAV:comp of the §9.6.1 selection with its
// children resolved by name. The selection is matched against every property of
// every component of every resource a report returns, so scanning it costs the
// request body multiplied by the collection -- the CPU exhaustion RFC 4791 §11
// asks a server to guard against. iCalendar names are case-insensitive, so the
// keys are upper-cased.
type calendarCompIndex struct {
	name    string
	allProp bool
	allComp bool
	// props and comps are nil under allProp and allComp respectively, where the
	// selection names no child to resolve.
	props map[string]calendarProp
	comps map[string]*calendarCompIndex
}

// newCalendarCompIndex resolves sel and everything below it. A name repeated
// among siblings keeps its first occurrence, which is the element a scan in
// document order would have stopped at.
func newCalendarCompIndex(sel *calendarComp) *calendarCompIndex {
	index := &calendarCompIndex{
		name:    sel.Name,
		allProp: sel.AllProp,
		allComp: sel.AllComp,
	}
	if !sel.AllProp && len(sel.Prop) > 0 {
		index.props = make(map[string]calendarProp, len(sel.Prop))
		for _, prop := range sel.Prop {
			key := strings.ToUpper(prop.Name)
			if _, seen := index.props[key]; !seen {
				index.props[key] = prop
			}
		}
	}
	if !sel.AllComp && len(sel.Comp) > 0 {
		index.comps = make(map[string]*calendarCompIndex, len(sel.Comp))
		for i := range sel.Comp {
			key := strings.ToUpper(sel.Comp[i].Name)
			if _, seen := index.comps[key]; !seen {
				index.comps[key] = newCalendarCompIndex(&sel.Comp[i])
			}
		}
	}
	return index
}

// selectComponent returns the copy of node that sel admits: the properties
// under (allprop | prop*) and the sub-components under (allcomp | comp*), per
// RFC 4791 §9.6.1. A bare CALDAV:comp names neither, and matching zero of each
// is what its content model says, so it returns the component alone. Properties
// named in mandatory survive whatever the selection said. nil means sel names
// no component matching node.
func selectComponent(node *icalNode, sel *calendarCompIndex, mandatory icalNameSet) *icalNode {
	if !strings.EqualFold(sel.name, node.name) {
		return nil
	}
	projected := &icalNode{name: node.name}
	if sel.allProp {
		projected.properties = append(projected.properties, node.properties...)
	} else {
		for _, property := range node.properties {
			if mandatory.contains(property.name) {
				projected.properties = append(projected.properties, property)
				continue
			}
			if selected, ok := selectProperty(property, sel.props); ok {
				projected.properties = append(projected.properties, selected)
			}
		}
	}
	if sel.allComp {
		projected.children = append(projected.children, node.children...)
		return projected
	}
	for _, child := range node.children {
		selectedChild, ok := sel.comps[strings.ToUpper(child.name)]
		if !ok {
			continue
		}
		if projectedChild := selectComponent(child, selectedChild, mandatory); projectedChild != nil {
			projected.children = append(projected.children, projectedChild)
		}
	}
	return projected
}

// selectProperty matches one content line against the CALDAV:prop selection,
// keeping every occurrence of a named property rather than the first: two
// ATTENDEEs are two content lines and §9.6.4 names neither of them individually.
func selectProperty(property icalProperty, selected map[string]calendarProp) (icalProperty, bool) {
	want, ok := selected[strings.ToUpper(property.name)]
	if !ok {
		return icalProperty{}, false
	}
	if want.NoValue {
		property.value = ""
	}
	return property, true
}
