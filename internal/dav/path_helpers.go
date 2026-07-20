package dav

// parseCalendarResourceSegments extracts the calendar collection segment and resource name.
// It accepts collection segments as either numeric IDs or slugs.
func parseCalendarResourceSegments(rawPath string) (string, string, bool) {
	target := parseDAVTarget(rawPath)
	if !target.Valid || target.Domain != davPathCalendar || !target.Resource {
		return "", "", false
	}
	return target.CollectionSegment, target.ResourceName, true
}
