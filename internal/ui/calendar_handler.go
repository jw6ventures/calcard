package ui

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"gitea.jw6.us/james/calcard/internal/auth"
	"gitea.jw6.us/james/calcard/internal/store"
	"gitea.jw6.us/james/calcard/internal/ui/utils"
	"github.com/go-chi/chi/v5"
)

// Calendars displays the user's calendars.
func (h *Handler) Calendars(w http.ResponseWriter, r *http.Request) {
	user, _ := auth.UserFromContext(r.Context())
	calendars, err := h.store.Calendars.ListAccessible(r.Context(), user.ID)
	if err != nil {
		http.Error(w, "failed to load calendars", http.StatusInternalServerError)
		return
	}
	users, err := h.store.Users.ListActive(r.Context())
	if err != nil {
		http.Error(w, "failed to load users", http.StatusInternalServerError)
		return
	}

	userMap := make(map[int64]store.User)
	for _, u := range users {
		userMap[u.ID] = u
	}

	type shareView struct {
		User      store.User
		Editor    bool
		CreatedAt time.Time
	}
	type calendarView struct {
		Access          store.CalendarAccess
		Shares          []shareView
		ShareCandidates []store.User
	}

	var items []calendarView
	for _, cal := range calendars {
		cv := calendarView{Access: cal}
		if !cal.Shared {
			shares, err := h.store.CalendarShares.ListByCalendar(r.Context(), cal.ID)
			if err != nil {
				http.Error(w, "failed to load shares", http.StatusInternalServerError)
				return
			}
			sharedUsers := make(map[int64]struct{})
			for _, s := range shares {
				if u, ok := userMap[s.UserID]; ok {
					cv.Shares = append(cv.Shares, shareView{User: u, Editor: s.Editor, CreatedAt: s.CreatedAt})
					sharedUsers[u.ID] = struct{}{}
				}
			}

			for _, candidate := range users {
				if candidate.ID == user.ID {
					continue
				}
				if _, ok := sharedUsers[candidate.ID]; ok {
					continue
				}
				cv.ShareCandidates = append(cv.ShareCandidates, candidate)
			}
		}
		items = append(items, cv)
	}

	data := h.withFlash(r, map[string]any{
		"Title":          "Calendars",
		"User":           user,
		"Calendars":      calendars,
		"CalendarViews":  items,
		"ActiveUsers":    users,
		"ShareableUsers": users,
	})
	h.render(w, "calendars.html", data)
}

// CreateCalendar creates a new calendar.
func (h *Handler) CreateCalendar(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		h.redirect(w, r, "/calendars", map[string]string{"error": "invalid form"})
		return
	}
	name := strings.TrimSpace(r.FormValue("name"))
	if name == "" {
		h.redirect(w, r, "/calendars", map[string]string{"error": "name is required"})
		return
	}

	user, _ := auth.UserFromContext(r.Context())
	_, err := h.store.Calendars.Create(r.Context(), store.Calendar{UserID: user.ID, Name: name})
	if err != nil {
		h.redirect(w, r, "/calendars", map[string]string{"error": "failed to create"})
		return
	}
	h.redirect(w, r, "/calendars", map[string]string{"status": "created"})
}

// RenameCalendar renames an existing calendar.
func (h *Handler) RenameCalendar(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		h.redirect(w, r, "/calendars", map[string]string{"error": "invalid form"})
		return
	}
	name := strings.TrimSpace(r.FormValue("name"))
	if name == "" {
		h.redirect(w, r, "/calendars", map[string]string{"error": "name is required"})
		return
	}
	user, _ := auth.UserFromContext(r.Context())
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		h.redirect(w, r, "/calendars", map[string]string{"error": "invalid id"})
		return
	}
	cal, err := h.store.Calendars.GetByID(r.Context(), id)
	if err != nil {
		h.redirect(w, r, "/calendars", map[string]string{"error": "rename failed"})
		return
	}
	if cal == nil || cal.UserID != user.ID {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if err := h.store.Calendars.Rename(r.Context(), user.ID, id, name); err != nil {
		h.redirect(w, r, "/calendars", map[string]string{"error": "rename failed"})
		return
	}
	h.redirect(w, r, "/calendars", map[string]string{"status": "renamed"})
}

// DeleteCalendar deletes a calendar.
func (h *Handler) DeleteCalendar(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		h.redirect(w, r, "/calendars", map[string]string{"error": "invalid id"})
		return
	}
	user, _ := auth.UserFromContext(r.Context())
	if err := h.store.Calendars.Delete(r.Context(), user.ID, id); err != nil {
		h.redirect(w, r, "/calendars", map[string]string{"error": "delete failed"})
		return
	}
	h.redirect(w, r, "/calendars", map[string]string{"status": "deleted"})
}

// ShareCalendar shares a calendar with another user.
func (h *Handler) ShareCalendar(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		h.redirect(w, r, "/calendars", map[string]string{"error": "invalid form"})
		return
	}
	user, _ := auth.UserFromContext(r.Context())
	calendarID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		h.redirect(w, r, "/calendars", map[string]string{"error": "invalid calendar"})
		return
	}
	targetID, err := strconv.ParseInt(r.FormValue("user_id"), 10, 64)
	if err != nil || targetID == 0 {
		h.redirect(w, r, "/calendars", map[string]string{"error": "invalid user"})
		return
	}
	if targetID == user.ID {
		h.redirect(w, r, "/calendars", map[string]string{"error": "cannot share with yourself"})
		return
	}

	cal, err := h.store.Calendars.GetByID(r.Context(), calendarID)
	if err != nil || cal == nil || cal.UserID != user.ID {
		h.redirect(w, r, "/calendars", map[string]string{"error": "not found"})
		return
	}

	targetUser, err := h.store.Users.GetByID(r.Context(), targetID)
	if err != nil || targetUser == nil {
		h.redirect(w, r, "/calendars", map[string]string{"error": "user not found"})
		return
	}

	if err := h.store.CalendarShares.Create(r.Context(), store.CalendarShare{
		CalendarID: cal.ID,
		UserID:     targetUser.ID,
		GrantedBy:  user.ID,
		Editor:     true,
	}); err != nil {
		h.redirect(w, r, "/calendars", map[string]string{"error": "failed to share"})
		return
	}

	h.redirect(w, r, "/calendars", map[string]string{"status": "shared"})
}

// UnshareCalendar removes a share or allows a user to leave a shared calendar.
func (h *Handler) UnshareCalendar(w http.ResponseWriter, r *http.Request) {
	user, _ := auth.UserFromContext(r.Context())
	calendarID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		h.redirect(w, r, "/calendars", map[string]string{"error": "invalid calendar"})
		return
	}
	targetID, err := strconv.ParseInt(chi.URLParam(r, "userId"), 10, 64)
	if err != nil || targetID == 0 {
		h.redirect(w, r, "/calendars", map[string]string{"error": "invalid user"})
		return
	}

	calAccess, err := h.store.Calendars.GetAccessible(r.Context(), calendarID, user.ID)
	if err != nil || calAccess == nil {
		h.redirect(w, r, "/calendars", map[string]string{"error": "not found"})
		return
	}

	if calAccess.UserID == user.ID {
		// Owner removing a share
		if err := h.store.CalendarShares.Delete(r.Context(), calendarID, targetID); err != nil {
			h.redirect(w, r, "/calendars", map[string]string{"error": "failed to unshare"})
			return
		}
	} else {
		// Shared user leaving
		if targetID != user.ID {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		if err := h.store.CalendarShares.Delete(r.Context(), calendarID, user.ID); err != nil {
			h.redirect(w, r, "/calendars", map[string]string{"error": "failed to leave"})
			return
		}
	}

	h.redirect(w, r, "/calendars", map[string]string{"status": "updated"})
}

// ViewCalendar displays a calendar and its events.
func (h *Handler) ViewCalendar(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		http.Error(w, "invalid calendar id", http.StatusBadRequest)
		return
	}
	user, _ := auth.UserFromContext(r.Context())
	cal, err := h.store.Calendars.GetAccessible(r.Context(), id, user.ID)
	if err != nil {
		http.Error(w, "failed to load calendar", http.StatusInternalServerError)
		return
	}
	if cal == nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	// Initial load - no events, will be loaded via AJAX
	data := h.withFlash(r, map[string]any{
		"Title":    cal.Name + " - Calendar",
		"User":     user,
		"Calendar": cal,
	})
	h.render(w, "calendar_view.html", data)
}

// GetCalendarEventsJSON returns events for a specific month in JSON format.
func (h *Handler) GetCalendarEventsJSON(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		http.Error(w, "invalid calendar id", http.StatusBadRequest)
		return
	}

	// Parse month and year from query params
	year, _ := strconv.Atoi(r.URL.Query().Get("year"))
	month, _ := strconv.Atoi(r.URL.Query().Get("month"))
	if year == 0 || month < 1 || month > 12 {
		now := time.Now()
		year = now.Year()
		month = int(now.Month())
	}

	user, _ := auth.UserFromContext(r.Context())
	cal, err := h.store.Calendars.GetAccessible(r.Context(), id, user.ID)
	if err != nil {
		http.Error(w, "failed to load calendar", http.StatusInternalServerError)
		return
	}
	if cal == nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	// Fetch all events for the calendar
	allEvents, err := h.store.Events.ListForCalendar(r.Context(), id)
	if err != nil {
		http.Error(w, "failed to load events", http.StatusInternalServerError)
		return
	}

	// Filter events relevant to the requested month
	// Include: one-time events in this month, and recurring events that could have occurrences
	monthStart := time.Date(year, time.Month(month), 1, 0, 0, 0, 0, time.UTC)
	monthEnd := monthStart.AddDate(0, 1, 0).Add(-time.Second)

	var relevantEvents []store.Event
	for _, ev := range allEvents {
		// Check if this is a recurring event (has RRULE)
		hasRRule := strings.Contains(ev.RawICAL, "RRULE:")

		if hasRRule {
			// For recurring events, include if they could have occurrences in this month
			// Check if event starts before month ends
			if ev.DTStart != nil && !ev.DTStart.After(monthEnd) {
				// Check if event has UNTIL and it's before month starts
				hasEnded := false
				lines := utils.UnfoldLines(ev.RawICAL)
				for _, line := range lines {
					if strings.HasPrefix(line, "RRULE:") && strings.Contains(line, "UNTIL=") {
						// Extract UNTIL date (simplified)
						parts := strings.Split(line, "UNTIL=")
						if len(parts) > 1 {
							untilStr := strings.Split(parts[1], ";")[0]
							untilStr = strings.Split(untilStr, "T")[0] // Get just the date part
							if len(untilStr) >= 8 {
								untilYear, _ := strconv.Atoi(untilStr[0:4])
								untilMonth, _ := strconv.Atoi(untilStr[4:6])
								untilDay, _ := strconv.Atoi(untilStr[6:8])
								untilDate := time.Date(untilYear, time.Month(untilMonth), untilDay, 0, 0, 0, 0, time.UTC)
								if untilDate.Before(monthStart) {
									hasEnded = true
								}
							}
						}
					}
				}
				if !hasEnded {
					relevantEvents = append(relevantEvents, ev)
				}
			}
		} else {
			// For one-time events, check if they fall in this month
			if ev.DTStart != nil {
				evStart := time.Date(ev.DTStart.Year(), ev.DTStart.Month(), ev.DTStart.Day(), 0, 0, 0, 0, time.UTC)
				if !evStart.Before(monthStart) && !evStart.After(monthEnd) {
					relevantEvents = append(relevantEvents, ev)
				}
			}
		}
	}

	// Build JSON response
	var eventsJSONData []map[string]any
	for _, ev := range relevantEvents {
		eventsJSONData = append(eventsJSONData, map[string]any{
			"uid":     ev.UID,
			"ical":    ev.RawICAL,
			"lastMod": ev.LastModified.Format(time.RFC3339),
		})
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(eventsJSONData); err != nil {
		http.Error(w, "failed to encode events", http.StatusInternalServerError)
	}
}

// ImportCalendar imports events from an ICS file into an existing calendar.
func (h *Handler) ImportCalendar(w http.ResponseWriter, r *http.Request) {
	calendarID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		http.Error(w, "invalid calendar id", http.StatusBadRequest)
		return
	}

	user, _ := auth.UserFromContext(r.Context())
	cal, err := h.store.Calendars.GetAccessible(r.Context(), calendarID, user.ID)
	if err != nil {
		http.Error(w, "failed to load calendar", http.StatusInternalServerError)
		return
	}
	if cal == nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if !cal.Editor {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	if err := r.ParseMultipartForm(10 << 20); err != nil {
		h.redirect(w, r, fmt.Sprintf("/calendars/%d", calendarID), map[string]string{"error": "invalid form data"})
		return
	}

	file, _, err := r.FormFile("ics_file")
	if err != nil {
		h.redirect(w, r, fmt.Sprintf("/calendars/%d", calendarID), map[string]string{"error": "no file uploaded"})
		return
	}
	defer file.Close()

	icsData, err := io.ReadAll(file)
	if err != nil {
		h.redirect(w, r, fmt.Sprintf("/calendars/%d", calendarID), map[string]string{"error": "failed to read file"})
		return
	}

	events := utils.ParseICSFile(string(icsData))
	if len(events) == 0 {
		h.redirect(w, r, fmt.Sprintf("/calendars/%d", calendarID), map[string]string{"error": "no events found in file"})
		return
	}

	imported := 0
	for _, eventICAL := range events {
		uid := utils.ExtractUID(eventICAL)
		if uid == "" {
			uid = utils.GenerateUID()
		}
		etag := utils.GenerateETag(eventICAL)

		if _, err := h.store.Events.Upsert(r.Context(), store.Event{
			CalendarID: calendarID,
			UID:        uid,
			ResourceName: uid,
			RawICAL:    eventICAL,
			ETag:       etag,
		}); err != nil {
			continue
		}
		imported++
	}

	if imported == 0 {
		h.redirect(w, r, fmt.Sprintf("/calendars/%d", calendarID), map[string]string{"error": "failed to import events"})
		return
	}

	h.redirect(w, r, fmt.Sprintf("/calendars/%d", calendarID), map[string]string{
		"status": fmt.Sprintf("imported_%d_events", imported),
	})
}

// CreateEvent creates a new event in a calendar.
func (h *Handler) CreateEvent(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}

	calendarID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		http.Error(w, "invalid calendar id", http.StatusBadRequest)
		return
	}

	user, _ := auth.UserFromContext(r.Context())
	cal, err := h.store.Calendars.GetAccessible(r.Context(), calendarID, user.ID)
	if err != nil {
		http.Error(w, "failed to load calendar", http.StatusInternalServerError)
		return
	}
	if cal == nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if !cal.Editor {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	summary := strings.TrimSpace(r.FormValue("summary"))
	if summary == "" {
		h.redirect(w, r, fmt.Sprintf("/calendars/%d", calendarID), map[string]string{"error": "summary is required"})
		return
	}

	dtstart := strings.TrimSpace(r.FormValue("dtstart"))
	dtend := strings.TrimSpace(r.FormValue("dtend"))
	allDay := r.FormValue("all_day") == "on"
	location := strings.TrimSpace(r.FormValue("location"))
	description := strings.TrimSpace(r.FormValue("description"))

	// Parse recurrence options
	recurrence := utils.ParseRecurrenceOptions(r)

	uid := utils.GenerateUID()
	ical := utils.BuildEvent(uid, summary, dtstart, dtend, allDay, location, description, recurrence)
	etag := utils.GenerateETag(ical)

	if _, err := h.store.Events.Upsert(r.Context(), store.Event{
		CalendarID:   calendarID,
		UID:          uid,
		ResourceName: uid,
		RawICAL:      ical,
		ETag:         etag,
	}); err != nil {
		h.redirect(w, r, fmt.Sprintf("/calendars/%d", calendarID), map[string]string{"error": "failed to create event"})
		return
	}

	h.redirect(w, r, fmt.Sprintf("/calendars/%d", calendarID), map[string]string{"status": "event_created"})
}

// UpdateEvent updates an existing event.
func (h *Handler) UpdateEvent(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}

	editScope := r.FormValue("edit_scope")
	recurrenceID := strings.TrimSpace(r.FormValue("recurrence_id"))
	recurrenceAllDay := r.FormValue("recurrence_all_day") == "true"

	calendarID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		http.Error(w, "invalid calendar id", http.StatusBadRequest)
		return
	}

	rawUID := chi.URLParam(r, "uid")
	uid, err := url.PathUnescape(rawUID)
	if err != nil || uid == "" {
		uid = rawUID
	}
	if uid == "" {
		http.Error(w, "invalid event uid", http.StatusBadRequest)
		return
	}

	user, _ := auth.UserFromContext(r.Context())
	cal, err := h.store.Calendars.GetAccessible(r.Context(), calendarID, user.ID)
	if err != nil {
		http.Error(w, "failed to load calendar", http.StatusInternalServerError)
		return
	}
	if cal == nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if !cal.Editor {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	existing, err := h.store.Events.GetByUID(r.Context(), calendarID, uid)
	if err != nil {
		http.Error(w, "failed to load event", http.StatusInternalServerError)
		return
	}
	if existing == nil {
		http.Error(w, "event not found", http.StatusNotFound)
		return
	}

	summary := strings.TrimSpace(r.FormValue("summary"))
	if summary == "" {
		h.redirect(w, r, fmt.Sprintf("/calendars/%d", calendarID), map[string]string{"error": "summary is required"})
		return
	}

	dtstart := strings.TrimSpace(r.FormValue("dtstart"))
	dtend := strings.TrimSpace(r.FormValue("dtend"))
	allDay := r.FormValue("all_day") == "on"
	location := strings.TrimSpace(r.FormValue("location"))
	description := strings.TrimSpace(r.FormValue("description"))

	// Parse recurrence options
	recurrence := utils.ParseRecurrenceOptions(r)

	ical := ""
	if editScope == "occurrence" && recurrenceID != "" && existing.RawICAL != "" {
		// Update only a single occurrence by replacing/adding a RECURRENCE-ID component.
		header, components, footer := utils.SplitComponents(existing.RawICAL)
		override := utils.BuildEventComponent(uid, summary, dtstart, dtend, allDay, location, description, nil, "")
		if recLine, err := utils.FormatICalDateTime(recurrenceID, recurrenceAllDay, false, "RECURRENCE-ID"); err == nil && recLine != "" {
			if len(override) >= 2 {
				override = append(override[:2], append([]string{recLine}, override[2:]...)...)
			} else {
				override = append([]string{recLine}, override...)
			}
		}
		targetRecID := utils.RecurrenceIDValue(override)

		var newComponents [][]string
		for _, comp := range components {
			if utils.RecurrenceIDValue(comp) == targetRecID {
				continue
			}
			newComponents = append(newComponents, comp)
		}
		newComponents = append(newComponents, override)
		ical = utils.BuildFromComponents(header, newComponents, footer)
	} else {
		// Update the series/master event while keeping overrides intact.
		header, components, footer := utils.SplitComponents(existing.RawICAL)
		master := utils.BuildEventComponent(uid, summary, dtstart, dtend, allDay, location, description, recurrence, "")
		replaced := false
		for i, comp := range components {
			if utils.RecurrenceIDValue(comp) == "" && !replaced {
				components[i] = master
				replaced = true
			}
		}
		if !replaced {
			components = append([][]string{master}, components...)
		}
		ical = utils.BuildFromComponents(header, components, footer)
	}
	etag := utils.GenerateETag(ical)

	resourceName := uid
	if existing != nil && existing.ResourceName != "" {
		resourceName = existing.ResourceName
	}
	if _, err := h.store.Events.Upsert(r.Context(), store.Event{
		CalendarID:   calendarID,
		UID:          uid,
		ResourceName: resourceName,
		RawICAL:      ical,
		ETag:         etag,
	}); err != nil {
		h.redirect(w, r, fmt.Sprintf("/calendars/%d", calendarID), map[string]string{"error": "failed to update event"})
		return
	}

	h.redirect(w, r, fmt.Sprintf("/calendars/%d", calendarID), map[string]string{"status": "event_updated"})
}

// DeleteEvent removes an event from a calendar.
func (h *Handler) DeleteEvent(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()
	editScope := r.FormValue("edit_scope")
	recurrenceID := strings.TrimSpace(r.FormValue("recurrence_id"))
	recurrenceAllDay := r.FormValue("recurrence_all_day") == "true"

	calendarID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		http.Error(w, "invalid calendar id", http.StatusBadRequest)
		return
	}

	rawUID := chi.URLParam(r, "uid")
	uid, err := url.PathUnescape(rawUID)
	if err != nil || uid == "" {
		uid = rawUID
	}
	if uid == "" {
		http.Error(w, "invalid event uid", http.StatusBadRequest)
		return
	}

	user, _ := auth.UserFromContext(r.Context())
	cal, err := h.store.Calendars.GetAccessible(r.Context(), calendarID, user.ID)
	if err != nil {
		http.Error(w, "failed to load calendar", http.StatusInternalServerError)
		return
	}
	if cal == nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if !cal.Editor {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	if editScope == "occurrence" && recurrenceID != "" {
		existing, err := h.store.Events.GetByUID(r.Context(), calendarID, uid)
		if err != nil {
			http.Error(w, "failed to load event", http.StatusInternalServerError)
			return
		}
		if existing == nil {
			http.Error(w, "event not found", http.StatusNotFound)
			return
		}

		exdateLine, err := utils.FormatICalDateTime(recurrenceID, recurrenceAllDay, false, "EXDATE")
		if err != nil || exdateLine == "" {
			h.redirect(w, r, fmt.Sprintf("/calendars/%d", calendarID), map[string]string{"error": "invalid recurrence id"})
			return
		}
		targetValueParts := strings.SplitN(exdateLine, ":", 2)
		if len(targetValueParts) != 2 {
			h.redirect(w, r, fmt.Sprintf("/calendars/%d", calendarID), map[string]string{"error": "invalid recurrence id"})
			return
		}
		targetValue := targetValueParts[1]

		header, components, footer := utils.SplitComponents(existing.RawICAL)
		var newComponents [][]string
		masterHandled := false
		for _, comp := range components {
			recID := utils.RecurrenceIDValue(comp)
			if recID == targetValue {
				// Drop an overridden occurrence matching the target.
				continue
			}
			if recID == "" && !masterHandled {
				if !utils.HasPropertyValue(comp, "EXDATE", targetValue) {
					comp = append(comp, exdateLine)
				}
				masterHandled = true
			}
			newComponents = append(newComponents, comp)
		}

		if !masterHandled {
			// No master to update; fall back to deleting the whole event.
			if err := h.store.Events.DeleteByUID(r.Context(), calendarID, uid); err != nil {
				h.redirect(w, r, fmt.Sprintf("/calendars/%d", calendarID), map[string]string{"error": "failed to delete event"})
				return
			}
			h.redirect(w, r, fmt.Sprintf("/calendars/%d", calendarID), map[string]string{"status": "event_deleted"})
			return
		}

		updatedICAL := utils.BuildFromComponents(header, newComponents, footer)
		resourceName := uid
		if existing != nil && existing.ResourceName != "" {
			resourceName = existing.ResourceName
		}
		if _, err := h.store.Events.Upsert(r.Context(), store.Event{
			CalendarID:   calendarID,
			UID:          uid,
			ResourceName: resourceName,
			RawICAL:      updatedICAL,
			ETag:         utils.GenerateETag(updatedICAL),
		}); err != nil {
			h.redirect(w, r, fmt.Sprintf("/calendars/%d", calendarID), map[string]string{"error": "failed to delete occurrence"})
			return
		}
		h.redirect(w, r, fmt.Sprintf("/calendars/%d", calendarID), map[string]string{"status": "occurrence_deleted"})
	} else {
		if err := h.store.Events.DeleteByUID(r.Context(), calendarID, uid); err != nil {
			h.redirect(w, r, fmt.Sprintf("/calendars/%d", calendarID), map[string]string{"error": "failed to delete event"})
			return
		}
		h.redirect(w, r, fmt.Sprintf("/calendars/%d", calendarID), map[string]string{"status": "event_deleted"})
	}
}
