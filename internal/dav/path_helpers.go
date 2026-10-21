package dav

// multiGetFallbackHref picks the DAV:href to report for a multiget entry that
// could not be resolved: the normalized path when there is one, otherwise the
// raw requested href, and the collection path as a last resort. A DAV:response
// must always carry a href, even for an href the server could not parse.
func multiGetFallbackHref(rawHref, cleanHref, collectionPath string) string {
	if cleanHref != "" {
		return cleanHref
	}
	if trimmed := trimHrefFraming(rawHref); trimmed != "" {
		return trimmed
	}
	return collectionPath
}

// parseCalendarResourceSegments extracts the calendar collection segment and resource name.
// It accepts collection segments as either numeric IDs or slugs.
func parseCalendarResourceSegments(rawPath string) (string, string, bool) {
	target := parseDAVTarget(rawPath)
	if !target.Valid || target.Domain != davPathCalendar || !target.Resource {
		return "", "", false
	}
	return target.CollectionSegment, target.ResourceName, true
}
