package ical

// Timezone is the zone tzid names within the object, read as
// ResolvePropertyTime reads it and through the same onsets, so a caller asking
// both of one object resolves each TZID once.
func (o *ObjectTimes) Timezone(tzid string) ObjectTimezone {
	return ObjectTimezone{definition: o.zone(tzid)}
}
