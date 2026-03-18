package events

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jw6ventures/calcard/internal/store"
	"github.com/jw6ventures/calcard/internal/ui/utils"
)

const MaxBodyBytes int64 = 10 * 1024 * 1024

var (
	ErrNotFound             = errors.New("not found")
	ErrForbidden            = errors.New("forbidden")
	ErrBadRequest           = errors.New("bad request")
	ErrConflict             = errors.New("conflict")
	ErrPreconditionFailed   = errors.New("precondition failed")
	ErrUnsupportedMediaType = errors.New("unsupported media type")
)

type Service struct {
	store *store.Store
}

func NewService(st *store.Store) *Service {
	return &Service{store: st}
}

type StructuredRecurrence struct {
	Frequency  string   `json:"frequency"`
	Interval   int      `json:"interval"`
	Count      int      `json:"count"`
	Until      string   `json:"until"`
	ByDay      []string `json:"byDay"`
	ByMonth    int      `json:"byMonth"`
	ByMonthDay int      `json:"byMonthDay"`
}

type StructuredInput struct {
	UID          string                `json:"uid"`
	Summary      string                `json:"summary"`
	DTStart      string                `json:"dtstart"`
	DTEnd        string                `json:"dtend"`
	AllDay       bool                  `json:"allDay"`
	Location     string                `json:"location"`
	Description  string                `json:"description"`
	Timezone     string                `json:"timezone"`
	URL          string                `json:"url"`
	Status       string                `json:"status"`
	Categories   []string              `json:"categories"`
	Class        string                `json:"class"`
	Transparency string                `json:"transparency"`
	Organizer    string                `json:"organizer"`
	Attendees    []string              `json:"attendees"`
	Attachments  []string              `json:"attachments"`
	Reminders    []int                 `json:"reminders"`
	Recurrence   *StructuredRecurrence `json:"recurrence"`
}

type UpsertInput struct {
	Structured  *StructuredInput
	RawICS      string
	ContentType string
	IfMatch     string
	IfNoneMatch string
}

func (s *Service) ListCalendars(ctx context.Context, user *store.User) ([]store.CalendarAccess, error) {
	return s.store.Calendars.ListAccessible(ctx, user.ID)
}

func (s *Service) GetCalendar(ctx context.Context, user *store.User, calendarID int64) (*store.CalendarAccess, error) {
	cal, err := s.store.Calendars.GetAccessible(ctx, calendarID, user.ID)
	if err != nil {
		return nil, err
	}
	if cal == nil {
		return nil, ErrNotFound
	}
	return cal, nil
}

func (s *Service) ListEvents(ctx context.Context, user *store.User, calendarID int64) ([]store.Event, error) {
	if _, err := s.GetCalendar(ctx, user, calendarID); err != nil {
		return nil, err
	}
	return s.store.Events.ListForCalendar(ctx, calendarID)
}

func (s *Service) GetEvent(ctx context.Context, user *store.User, calendarID int64, uid string) (*store.Event, error) {
	if _, err := s.GetCalendar(ctx, user, calendarID); err != nil {
		return nil, err
	}
	ev, err := s.store.Events.GetByUID(ctx, calendarID, uid)
	if err != nil {
		return nil, err
	}
	if ev == nil {
		return nil, ErrNotFound
	}
	return ev, nil
}

func (s *Service) CreateEvent(ctx context.Context, user *store.User, calendarID int64, input UpsertInput) (*store.Event, bool, error) {
	cal, err := s.GetCalendar(ctx, user, calendarID)
	if err != nil {
		return nil, false, err
	}
	if !cal.Editor {
		return nil, false, ErrForbidden
	}

	body, uid, err := s.normalizeEventPayload(input, "")
	if err != nil {
		return nil, false, err
	}
	existing, err := s.store.Events.GetByUID(ctx, calendarID, uid)
	if err != nil {
		return nil, false, err
	}
	if !checkConditionalHeaders(input.IfMatch, input.IfNoneMatch, existing) {
		return nil, false, ErrPreconditionFailed
	}
	if existing != nil {
		return nil, false, ErrConflict
	}

	event, created, err := s.saveEvent(ctx, calendarID, uid, uid, body, input.IfMatch, input.IfNoneMatch)
	return event, created, err
}

func (s *Service) UpdateEvent(ctx context.Context, user *store.User, calendarID int64, uid string, input UpsertInput) (*store.Event, bool, error) {
	cal, err := s.GetCalendar(ctx, user, calendarID)
	if err != nil {
		return nil, false, err
	}
	if !cal.Editor {
		return nil, false, ErrForbidden
	}

	existing, err := s.store.Events.GetByUID(ctx, calendarID, uid)
	if err != nil {
		return nil, false, err
	}
	if existing == nil {
		return nil, false, ErrNotFound
	}
	if !checkConditionalHeaders(input.IfMatch, input.IfNoneMatch, existing) {
		return nil, false, ErrPreconditionFailed
	}

	body, normalizedUID, err := s.normalizeEventPayload(input, uid)
	if err != nil {
		return nil, false, err
	}
	if normalizedUID != uid {
		return nil, false, fmt.Errorf("%w: uid mismatch", ErrBadRequest)
	}

	resourceName := existing.ResourceName
	if resourceName == "" {
		resourceName = uid
	}
	event, created, err := s.saveEvent(ctx, calendarID, uid, resourceName, body, input.IfMatch, input.IfNoneMatch)
	return event, created, err
}

func (s *Service) DeleteEvent(ctx context.Context, user *store.User, calendarID int64, uid, ifMatch, ifNoneMatch string) error {
	cal, err := s.GetCalendar(ctx, user, calendarID)
	if err != nil {
		return err
	}
	if !cal.Editor {
		return ErrForbidden
	}

	existing, err := s.store.Events.GetByUID(ctx, calendarID, uid)
	if err != nil {
		return err
	}
	if !checkConditionalHeaders(ifMatch, ifNoneMatch, existing) {
		return ErrPreconditionFailed
	}
	if existing == nil {
		return ErrNotFound
	}
	return s.store.Events.DeleteByUID(ctx, calendarID, uid)
}

func (s *Service) normalizeEventPayload(input UpsertInput, expectedUID string) (string, string, error) {
	if strings.TrimSpace(input.RawICS) != "" {
		if err := validateCalendarContentType(input.ContentType); err != nil {
			return "", "", err
		}
		body := strings.TrimSpace(input.RawICS)
		if err := validateStrictICalendar(body); err != nil {
			return "", "", err
		}
		uid, err := extractUIDFromICalendar(body)
		if err != nil {
			return "", "", fmt.Errorf("%w: invalid uid", ErrBadRequest)
		}
		if expectedUID != "" && uid != expectedUID {
			return "", "", fmt.Errorf("%w: path uid does not match calendar data uid", ErrBadRequest)
		}
		return ensureCRLF(body), uid, nil
	}
	if input.Structured == nil {
		return "", "", fmt.Errorf("%w: missing event body", ErrBadRequest)
	}

	body, uid, err := buildStructuredEvent(input.Structured, expectedUID)
	if err != nil {
		return "", "", err
	}
	if err := validateStrictICalendar(body); err != nil {
		return "", "", err
	}
	return body, uid, nil
}

func (s *Service) saveEvent(ctx context.Context, calendarID int64, uid, resourceName, body, ifMatch, ifNoneMatch string) (*store.Event, bool, error) {
	existingByResource, err := s.store.Events.GetByResourceName(ctx, calendarID, resourceName)
	if err != nil {
		return nil, false, err
	}
	if existingByResource != nil && existingByResource.UID != uid {
		return nil, false, ErrConflict
	}

	existing, err := s.store.Events.GetByUID(ctx, calendarID, uid)
	if err != nil {
		return nil, false, err
	}
	if existing != nil && existing.ResourceName != "" && existing.ResourceName != resourceName {
		return nil, false, ErrConflict
	}
	if !checkConditionalHeaders(ifMatch, ifNoneMatch, existing) {
		return nil, false, ErrPreconditionFailed
	}

	etag := fmt.Sprintf("%x", sha256.Sum256([]byte(body)))
	created := existing == nil
	ev, err := s.store.Events.Upsert(ctx, store.Event{
		CalendarID:   calendarID,
		UID:          uid,
		ResourceName: resourceName,
		RawICAL:      body,
		ETag:         etag,
	})
	if err != nil {
		return nil, false, err
	}
	return ev, created, nil
}

func buildStructuredEvent(input *StructuredInput, expectedUID string) (string, string, error) {
	summary := strings.TrimSpace(input.Summary)
	if summary == "" {
		return "", "", fmt.Errorf("%w: summary is required", ErrBadRequest)
	}
	dtstart := strings.TrimSpace(input.DTStart)
	dtend := strings.TrimSpace(input.DTEnd)
	if dtstart == "" || dtend == "" {
		return "", "", fmt.Errorf("%w: start and end are required", ErrBadRequest)
	}
	if err := validateEventDates(dtstart, dtend); err != nil {
		return "", "", fmt.Errorf("%w: %v", ErrBadRequest, err)
	}

	uid := strings.TrimSpace(input.UID)
	if expectedUID != "" {
		if uid != "" && uid != expectedUID {
			return "", "", fmt.Errorf("%w: path uid does not match payload uid", ErrBadRequest)
		}
		uid = expectedUID
	}
	if uid == "" {
		uid = utils.GenerateUID()
	}

	var recurrence *utils.RecurrenceOptions
	if input.Recurrence != nil {
		recurrence = &utils.RecurrenceOptions{
			Frequency:  strings.TrimSpace(input.Recurrence.Frequency),
			Interval:   input.Recurrence.Interval,
			Count:      input.Recurrence.Count,
			Until:      strings.TrimSpace(input.Recurrence.Until),
			ByDay:      input.Recurrence.ByDay,
			ByMonth:    input.Recurrence.ByMonth,
			ByMonthDay: input.Recurrence.ByMonthDay,
		}
	}

	opts := &utils.EventOptions{
		Timezone:     strings.TrimSpace(input.Timezone),
		URL:          strings.TrimSpace(input.URL),
		Status:       strings.TrimSpace(input.Status),
		Categories:   input.Categories,
		Class:        strings.TrimSpace(input.Class),
		Transparency: strings.TrimSpace(input.Transparency),
		Organizer:    strings.TrimSpace(input.Organizer),
		Attendees:    input.Attendees,
		Attachments:  input.Attachments,
		Reminders:    input.Reminders,
	}

	body := utils.BuildEvent(
		uid,
		summary,
		dtstart,
		dtend,
		input.AllDay,
		strings.TrimSpace(input.Location),
		strings.TrimSpace(input.Description),
		recurrence,
		opts,
	)
	return body, uid, nil
}

func validateEventDates(dtstart, dtend string) error {
	layouts := []string{
		"2006-01-02T15:04",
		"2006-01-02T15:04:05",
		"2006-01-02T15:04:05Z07:00",
		"2006-01-02",
	}

	var startTime, endTime time.Time
	var startErr, endErr error
	for _, layout := range layouts {
		startTime, startErr = time.Parse(layout, dtstart)
		if startErr == nil {
			break
		}
	}
	if startErr != nil {
		return fmt.Errorf("invalid start date format")
	}
	for _, layout := range layouts {
		endTime, endErr = time.Parse(layout, dtend)
		if endErr == nil {
			break
		}
	}
	if endErr != nil {
		return fmt.Errorf("invalid end date format")
	}
	if !endTime.After(startTime) {
		return fmt.Errorf("end date must be after start date")
	}
	return nil
}

func validateCalendarContentType(contentType string) error {
	contentType = strings.ToLower(strings.TrimSpace(contentType))
	if contentType == "" {
		return fmt.Errorf("%w: missing content type", ErrUnsupportedMediaType)
	}
	if strings.HasPrefix(contentType, "text/calendar") || strings.HasPrefix(contentType, "application/ical") || strings.HasPrefix(contentType, "application/ics") {
		return nil
	}
	return fmt.Errorf("%w: unsupported content type", ErrUnsupportedMediaType)
}

func checkConditionalHeaders(ifMatch, ifNoneMatch string, existing *store.Event) bool {
	if ifNoneMatch == "*" {
		return existing == nil
	}
	if ifMatch != "" {
		if existing == nil {
			return false
		}
		return strings.Trim(ifMatch, "\"") == existing.ETag
	}
	if ifNoneMatch != "" {
		if existing == nil {
			return true
		}
		return strings.Trim(ifNoneMatch, "\"") != existing.ETag
	}
	return true
}

func validateStrictICalendar(data string) error {
	if err := validateICalendar(data); err != nil {
		return fmt.Errorf("%w: invalid calendar data", ErrBadRequest)
	}
	componentTypes := extractICalComponentTypes(data)
	allowedComponents := map[string]struct{}{
		"VCALENDAR": {},
		"VEVENT":    {},
		"VTODO":     {},
		"VJOURNAL":  {},
		"VFREEBUSY": {},
		"VTIMEZONE": {},
		"STANDARD":  {},
		"DAYLIGHT":  {},
		"VALARM":    {},
	}
	for comp := range componentTypes {
		if _, ok := allowedComponents[comp]; !ok {
			return fmt.Errorf("%w: unsupported calendar component", ErrBadRequest)
		}
	}
	_, hasEvent := componentTypes["VEVENT"]
	_, hasTodo := componentTypes["VTODO"]
	_, hasJournal := componentTypes["VJOURNAL"]
	_, hasFreeBusy := componentTypes["VFREEBUSY"]
	if !hasEvent && !hasTodo && !hasJournal && !hasFreeBusy {
		return fmt.Errorf("%w: missing supported calendar component", ErrBadRequest)
	}
	if containsICalMethodProperty(data) {
		return fmt.Errorf("%w: METHOD property not allowed", ErrConflict)
	}
	if conditions := validateCalendarObjectResource(data); len(conditions) > 0 {
		if hasMultipleDifferentUIDs(data) {
			return fmt.Errorf("%w: multiple UIDs in single resource", ErrConflict)
		}
		return fmt.Errorf("%w: invalid calendar object resource", ErrBadRequest)
	}
	minDate, maxDate := caldavDateLimits()
	for _, t := range extractICalDateTimes(data) {
		if t.Before(minDate) || t.After(maxDate) {
			return fmt.Errorf("%w: date outside supported range", ErrBadRequest)
		}
	}
	if attendeeCount := countICalAttendees(data); attendeeCount > caldavMaxAttendees {
		return fmt.Errorf("%w: too many attendees", ErrBadRequest)
	}
	if count, ok := extractICalRRULECount(data); ok && count > caldavMaxInstances {
		return fmt.Errorf("%w: too many recurrence instances", ErrBadRequest)
	}
	return nil
}

func validateICalendar(data string) error {
	trimmed := strings.TrimSpace(data)
	if !strings.HasPrefix(strings.ToUpper(trimmed), "BEGIN:VCALENDAR") {
		return fmt.Errorf("missing BEGIN:VCALENDAR")
	}
	if !strings.HasSuffix(strings.ToUpper(trimmed), "END:VCALENDAR") {
		return fmt.Errorf("missing END:VCALENDAR")
	}
	upper := strings.ToUpper(trimmed)
	beginCount := strings.Count(upper, "BEGIN:VCALENDAR")
	endCount := strings.Count(upper, "END:VCALENDAR")
	if beginCount != endCount {
		return fmt.Errorf("unbalanced VCALENDAR tags")
	}
	componentTypes := extractICalComponentTypes(data)
	if _, ok := componentTypes["VEVENT"]; !ok {
		if _, ok := componentTypes["VTODO"]; !ok {
			if _, ok := componentTypes["VJOURNAL"]; !ok {
				if _, ok := componentTypes["VFREEBUSY"]; !ok {
					return fmt.Errorf("missing supported calendar component")
				}
			}
		}
	}
	return nil
}

func extractICalComponentTypes(icalData string) map[string]struct{} {
	componentTypes := make(map[string]struct{})
	lines := strings.Split(icalData, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(strings.TrimSuffix(line, "\r"))
		upperLine := strings.ToUpper(line)
		if strings.HasPrefix(upperLine, "BEGIN:") {
			componentType := strings.TrimSpace(upperLine[6:])
			if componentType != "" {
				componentTypes[componentType] = struct{}{}
			}
		}
	}
	return componentTypes
}

func extractICalRRULECount(icalData string) (int, bool) {
	lines := strings.Split(icalData, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(strings.TrimSuffix(line, "\r"))
		upperLine := strings.ToUpper(line)
		if !strings.HasPrefix(upperLine, "RRULE:") {
			continue
		}
		rrule := line[len("RRULE:"):]
		parts := strings.Split(rrule, ";")
		for _, part := range parts {
			kv := strings.SplitN(strings.TrimSpace(part), "=", 2)
			if len(kv) != 2 || !strings.EqualFold(kv[0], "COUNT") {
				continue
			}
			count, err := strconv.Atoi(kv[1])
			if err != nil {
				return 0, false
			}
			return count, true
		}
	}
	return 0, false
}

func countICalAttendees(icalData string) int {
	count := 0
	lines := utils.UnfoldLines(icalData)
	for _, line := range lines {
		upper := strings.ToUpper(strings.TrimSpace(line))
		if strings.HasPrefix(upper, "ATTENDEE") {
			count++
		}
	}
	return count
}

func extractUIDFromICalendar(icalData string) (string, error) {
	lines := utils.UnfoldLines(icalData)
	seenUIDs := make(map[string]struct{})
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		upper := strings.ToUpper(trimmed)
		if !strings.HasPrefix(upper, "UID:") {
			continue
		}
		uid := strings.TrimSpace(trimmed[4:])
		if uid == "" {
			return "", fmt.Errorf("empty uid")
		}
		seenUIDs[uid] = struct{}{}
	}
	switch len(seenUIDs) {
	case 0:
		return "", fmt.Errorf("missing uid")
	case 1:
		for uid := range seenUIDs {
			return uid, nil
		}
	}
	return "", fmt.Errorf("multiple uids")
}

func validateCalendarObjectResource(icalData string) []string {
	var conditions []string
	lines := utils.UnfoldLines(icalData)
	inEvent := false
	currentUID := ""
	seenUID := false
	seenUIDs := make(map[string]struct{})
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		upper := strings.ToUpper(trimmed)
		switch upper {
		case "BEGIN:VEVENT", "BEGIN:VTODO", "BEGIN:VJOURNAL", "BEGIN:VFREEBUSY":
			inEvent = true
			currentUID = ""
			seenUID = false
		case "END:VEVENT", "END:VTODO", "END:VJOURNAL", "END:VFREEBUSY":
			if inEvent && !seenUID {
				conditions = append(conditions, "valid-calendar-object-resource")
			}
			if currentUID != "" {
				seenUIDs[currentUID] = struct{}{}
			}
			inEvent = false
		default:
			if inEvent && strings.HasPrefix(upper, "UID:") {
				currentUID = strings.TrimSpace(trimmed[4:])
				seenUID = currentUID != ""
			}
		}
	}
	if len(seenUIDs) > 1 {
		conditions = append(conditions, "no-uid-conflict")
	}
	return conditions
}

func hasMultipleDifferentUIDs(icalData string) bool {
	lines := utils.UnfoldLines(icalData)
	seenUIDs := make(map[string]struct{})
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		upper := strings.ToUpper(trimmed)
		if strings.HasPrefix(upper, "UID:") {
			uid := strings.TrimSpace(trimmed[4:])
			if uid != "" {
				seenUIDs[uid] = struct{}{}
			}
		}
	}
	return len(seenUIDs) > 1
}

func containsICalMethodProperty(icalData string) bool {
	lines := utils.UnfoldLines(icalData)
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		upper := strings.ToUpper(trimmed)
		if strings.HasPrefix(upper, "METHOD:") {
			return true
		}
	}
	return false
}

func extractICalDateTimes(ical string) []time.Time {
	var values []time.Time
	lines := utils.UnfoldLines(ical)
	for _, line := range lines {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" {
			continue
		}
		upper := strings.ToUpper(trimmed)
		if !strings.HasPrefix(upper, "DTSTART") && !strings.HasPrefix(upper, "DTEND") && !strings.HasPrefix(upper, "DUE") && !strings.HasPrefix(upper, "RECURRENCE-ID") {
			continue
		}
		parts := strings.SplitN(trimmed, ":", 2)
		if len(parts) != 2 {
			continue
		}
		value := strings.TrimSpace(parts[1])
		if value == "" {
			continue
		}
		if t, err := parseICalDateTime(value); err == nil {
			values = append(values, t)
		}
	}
	return values
}

func parseICalDateTime(s string) (time.Time, error) {
	formats := []string{
		"20060102",
		"20060102T150405",
		"20060102T150405Z",
		"20060102T150405-0700",
		"20060102T150405-07:00",
		"2006-01-02T15:04:05",
		"2006-01-02T15:04:05Z",
		"2006-01-02T15:04:05-0700",
		"2006-01-02T15:04:05-07:00",
	}
	for _, format := range formats {
		if t, err := time.Parse(format, s); err == nil {
			return t.UTC(), nil
		}
	}
	return time.Time{}, fmt.Errorf("invalid datetime format: %s", s)
}

const (
	caldavMaxInstances = 2000
	caldavMaxAttendees = 1000
)

func caldavDateLimits() (time.Time, time.Time) {
	minDate := time.Date(1900, 1, 1, 0, 0, 0, 0, time.UTC)
	maxDate := time.Date(2100, 12, 31, 23, 59, 59, 0, time.UTC)
	return minDate, maxDate
}

func ensureCRLF(body string) string {
	lines := utils.UnfoldLines(body)
	return strings.Join(lines, "\r\n") + "\r\n"
}

func StatusCode(err error) int {
	switch {
	case err == nil:
		return http.StatusOK
	case errors.Is(err, ErrNotFound):
		return http.StatusNotFound
	case errors.Is(err, ErrForbidden):
		return http.StatusForbidden
	case errors.Is(err, ErrConflict):
		return http.StatusConflict
	case errors.Is(err, ErrPreconditionFailed):
		return http.StatusPreconditionFailed
	case errors.Is(err, ErrUnsupportedMediaType):
		return http.StatusUnsupportedMediaType
	case errors.Is(err, ErrBadRequest):
		return http.StatusBadRequest
	default:
		return http.StatusInternalServerError
	}
}
