package dav

// validateICalendar is retained for package-local callers that only need the
// structural result of the shared analysis.
func (h *DavServer) validateICalendar(data string) error {
	_, err := analyzeICalendar(data)
	return err
}

// extractUIDFromICalendar is retained for package-local callers that only need
// the UID produced by the shared analysis.
func extractUIDFromICalendar(data string) (string, error) {
	analysis, err := analyzeICalendar(data)
	if err != nil {
		return "", err
	}
	return analysis.uid()
}

func isTopLevelComponentType(componentType string) bool {
	switch componentType {
	case "VEVENT", "VTODO", "VJOURNAL", "VFREEBUSY":
		return true
	default:
		return false
	}
}
