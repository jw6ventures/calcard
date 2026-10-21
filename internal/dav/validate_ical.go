package dav

func isTopLevelComponentType(componentType string) bool {
	switch componentType {
	case "VEVENT", "VTODO", "VJOURNAL", "VFREEBUSY":
		return true
	default:
		return nonStandardICalendarName(componentType)
	}
}
