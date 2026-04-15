package ui

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
	"github.com/jw6ventures/calcard/internal/ui/utils"
)

type calendarShareView struct {
	User      store.User
	Editor    bool
	CreatedAt time.Time
}

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

	type calendarView struct {
		Access          store.CalendarAccess
		Shares          []calendarShareView
		ShareCandidates []store.User
	}

	var items []calendarView
	for _, cal := range calendars {
		cv := calendarView{Access: cal}
		if !cal.Shared {
			shares, err := h.calendarShareViews(r.Context(), cal.ID, userMap)
			if err != nil {
				http.Error(w, "failed to load shares", http.StatusInternalServerError)
				return
			}
			cv.Shares = shares
			sharedUsers := make(map[int64]struct{}, len(shares))
			for _, s := range shares {
				sharedUsers[s.User.ID] = struct{}{}
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
	h.render(w, r, "calendars.html", data)
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
	color, err := calendarColorFromForm(r, nil)
	if err != nil {
		h.redirect(w, r, "/calendars", map[string]string{"error": "invalid color"})
		return
	}

	user, _ := auth.UserFromContext(r.Context())
	_, err = h.store.Calendars.Create(r.Context(), store.Calendar{UserID: user.ID, Name: name, Color: color})
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
	color := cal.Color
	if _, ok := r.Form["color"]; ok {
		color, err = calendarColorFromForm(r, cal.Color)
		if err != nil {
			h.redirect(w, r, "/calendars", map[string]string{"error": "invalid color"})
			return
		}
	}
	if err := h.store.Calendars.Update(r.Context(), user.ID, id, name, cal.Description, cal.Timezone, color); err != nil {
		h.redirect(w, r, "/calendars", map[string]string{"error": "rename failed"})
		return
	}
	h.redirect(w, r, "/calendars", map[string]string{"status": "renamed"})
}

func calendarColorFromForm(r *http.Request, existing *string) (*string, error) {
	color, err := store.NormalizeCalendarColor(r.FormValue("color"))
	if err != nil || color == nil {
		return color, err
	}
	if len(*color) == 9 {
		return color, nil
	}

	alphaValue := strings.TrimSpace(r.FormValue("color_alpha"))
	if alphaValue == "" {
		if existing != nil && len(*existing) == 9 && strings.EqualFold((*existing)[:7], *color) {
			preserved := strings.ToUpper(*color + (*existing)[7:])
			return &preserved, nil
		}
		return store.NormalizeCalendarColorOpaque(*color)
	}

	alpha, err := strconv.Atoi(alphaValue)
	if err != nil || alpha < 0 || alpha > 100 {
		return nil, fmt.Errorf("invalid color alpha")
	}
	normalized := fmt.Sprintf("%s%02X", *color, (alpha*255+50)/100)
	return &normalized, nil
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

	if err := h.setCalendarShare(r.Context(), cal.ID, targetUser.ID, true); err != nil {
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
		if err := h.removeCalendarShare(r.Context(), calendarID, targetID); err != nil {
			h.redirect(w, r, "/calendars", map[string]string{"error": "failed to unshare"})
			return
		}
	} else {
		// Shared user leaving
		if targetID != user.ID {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		if err := h.removeCalendarShare(r.Context(), calendarID, user.ID); err != nil {
			h.redirect(w, r, "/calendars", map[string]string{"error": "failed to leave"})
			return
		}
	}

	h.redirect(w, r, "/calendars", map[string]string{"status": "updated"})
}

func calendarACLResourcePath(calendarID int64) string {
	return path.Join("/dav/calendars", fmt.Sprint(calendarID))
}

func calendarSharePrincipalHref(userID int64) string {
	return fmt.Sprintf("/dav/principals/%d/", userID)
}

func calendarEventResourcePath(calendarID int64, resourceName string) string {
	return path.Join(calendarACLResourcePath(calendarID), resourceName)
}

func calendarACLLookupPaths(resourcePath string) []string {
	resourcePath = path.Clean(resourcePath)
	if resourcePath == "." || resourcePath == "/" || resourcePath == "" {
		return nil
	}

	seen := map[string]struct{}{}
	paths := make([]string, 0, 3)
	addPath := func(candidate string) {
		candidate = path.Clean(candidate)
		if candidate == "." || candidate == "/" || candidate == "" {
			return
		}
		if _, ok := seen[candidate]; ok {
			return
		}
		seen[candidate] = struct{}{}
		paths = append(paths, candidate)
	}

	addPath(resourcePath)
	if ext := path.Ext(resourcePath); strings.EqualFold(ext, ".ics") {
		addPath(strings.TrimSuffix(resourcePath, ext))
	} else if strings.Count(strings.TrimPrefix(resourcePath, "/dav/calendars/"), "/") == 1 {
		addPath(resourcePath + ".ics")
	}

	return paths
}

func calendarEventResourceName(uid string, existing *store.Event) string {
	if existing != nil && existing.ResourceName != "" {
		return existing.ResourceName
	}
	return utils.ResourceNameForUID(uid)
}

func calendarACLPrivilegeMatches(granted, requested string) bool {
	if granted == requested || granted == "all" {
		return true
	}
	if granted == "read" && requested == "read-free-busy" {
		return true
	}
	return granted == "write" && (requested == "write-content" || requested == "write-properties" || requested == "bind" || requested == "unbind")
}

func calendarShareManagedPrivilege(privilege string) bool {
	switch privilege {
	case "read", "read-free-busy", "write":
		return true
	default:
		return false
	}
}

func calendarShareVisiblePrivilege(privilege string) bool {
	switch privilege {
	case "read", "read-free-busy", "write", "write-content", "write-properties", "bind", "unbind", "all":
		return true
	default:
		return false
	}
}

func calendarSharePresetEntries(calendarID, userID int64, editor bool) []store.ACLEntry {
	privileges := []string{"read", "read-free-busy"}
	if editor {
		privileges = append(privileges, "write")
	}

	resourcePath := calendarACLResourcePath(calendarID)
	principalHref := calendarSharePrincipalHref(userID)
	entries := make([]store.ACLEntry, 0, len(privileges))
	for _, privilege := range privileges {
		entries = append(entries, store.ACLEntry{
			ResourcePath:  resourcePath,
			PrincipalHref: principalHref,
			IsGrant:       true,
			Privilege:     privilege,
		})
	}
	return entries
}

func shareEditorFromACLEntries(entries []store.ACLEntry) bool {
	for _, entry := range entries {
		if !entry.IsGrant {
			continue
		}
		switch entry.Privilege {
		case "write", "write-content", "write-properties", "bind", "unbind", "all":
			return true
		}
	}
	return false
}

func (h *Handler) requireCalendarPrivilege(ctx context.Context, user *store.User, cal *store.CalendarAccess, resourcePath, privilege string) error {
	if cal == nil || user == nil {
		return store.ErrNotFound
	}
	if cal.UserID == user.ID {
		return nil
	}
	if h == nil || h.store == nil || h.store.ACLEntries == nil {
		if cal.EffectivePrivileges().Allows(privilege) {
			return nil
		}
		return store.ErrNotFound
	}

	applicablePrincipals := map[string]struct{}{
		"DAV:all":                           {},
		"DAV:authenticated":                 {},
		calendarSharePrincipalHref(user.ID): {},
	}
	candidates := calendarACLLookupPaths(resourcePath)
	candidates = append(candidates, calendarACLResourcePath(cal.ID))
	for _, candidate := range candidates {
		entries, err := h.store.ACLEntries.ListByResource(ctx, candidate)
		if err != nil {
			return err
		}

		hasGrant := false
		for _, entry := range entries {
			if _, ok := applicablePrincipals[entry.PrincipalHref]; !ok {
				continue
			}
			if !calendarACLPrivilegeMatches(entry.Privilege, privilege) {
				continue
			}
			if !entry.IsGrant {
				return store.ErrNotFound
			}
			hasGrant = true
		}
		if hasGrant {
			return nil
		}
	}

	return store.ErrNotFound
}

func (h *Handler) canReadCalendarEvent(ctx context.Context, user *store.User, cal *store.CalendarAccess, event store.Event) (bool, error) {
	if err := h.requireCalendarPrivilege(ctx, user, cal, calendarEventResourcePath(cal.ID, calendarEventResourceName(event.UID, &event)), "read"); err != nil {
		if err == store.ErrNotFound {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (h *Handler) filterReadableCalendarEvents(ctx context.Context, user *store.User, cal *store.CalendarAccess, events []store.Event) ([]store.Event, error) {
	visible := make([]store.Event, 0, len(events))
	for _, event := range events {
		allowed, err := h.canReadCalendarEvent(ctx, user, cal, event)
		if err != nil {
			return nil, err
		}
		if allowed {
			visible = append(visible, event)
		}
	}
	return visible, nil
}

func (h *Handler) calendarShareViews(ctx context.Context, calendarID int64, userMap map[int64]store.User) ([]calendarShareView, error) {
	resourcePath := calendarACLResourcePath(calendarID)
	entries, err := h.store.ACLEntries.ListByResource(ctx, resourcePath)
	if err != nil {
		return nil, err
	}

	grouped := map[int64][]store.ACLEntry{}
	createdAt := map[int64]time.Time{}
	for _, entry := range entries {
		if !entry.IsGrant || !calendarShareVisiblePrivilege(entry.Privilege) || !strings.HasPrefix(entry.PrincipalHref, "/dav/principals/") || !strings.HasSuffix(entry.PrincipalHref, "/") {
			continue
		}
		rawID := strings.TrimSuffix(strings.TrimPrefix(entry.PrincipalHref, "/dav/principals/"), "/")
		userID, err := strconv.ParseInt(rawID, 10, 64)
		if err != nil {
			continue
		}
		grouped[userID] = append(grouped[userID], entry)
		if createdAt[userID].IsZero() || entry.CreatedAt.Before(createdAt[userID]) {
			createdAt[userID] = entry.CreatedAt
		}
	}

	shares := make([]calendarShareView, 0, len(grouped))
	for userID, shareEntries := range grouped {
		u, ok := userMap[userID]
		if !ok {
			continue
		}
		shares = append(shares, calendarShareView{
			User:      u,
			Editor:    shareEditorFromACLEntries(shareEntries),
			CreatedAt: createdAt[userID],
		})
	}
	sort.Slice(shares, func(i, j int) bool {
		return shares[i].User.PrimaryEmail < shares[j].User.PrimaryEmail
	})
	return shares, nil
}

func (h *Handler) setCalendarShare(ctx context.Context, calendarID, userID int64, editor bool) error {
	resourcePath := calendarACLResourcePath(calendarID)
	entries, err := h.store.ACLEntries.ListByResource(ctx, resourcePath)
	if err != nil {
		return err
	}
	principalHref := calendarSharePrincipalHref(userID)
	filtered := make([]store.ACLEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.PrincipalHref == principalHref && entry.IsGrant && calendarShareManagedPrivilege(entry.Privilege) {
			continue
		}
		filtered = append(filtered, entry)
	}
	filtered = append(filtered, calendarSharePresetEntries(calendarID, userID, editor)...)
	return h.store.ACLEntries.SetACL(ctx, resourcePath, filtered)
}

func (h *Handler) removeCalendarShare(ctx context.Context, calendarID, userID int64) error {
	return h.store.ACLEntries.DeletePrincipalEntriesByResourcePrefix(ctx, calendarSharePrincipalHref(userID), calendarACLResourcePath(calendarID))
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
	calendarCapabilities := cal.EffectivePrivileges()
	data := h.withFlash(r, map[string]any{
		"Title":                cal.Name + " - Calendar",
		"User":                 user,
		"Calendar":             cal,
		"CalendarCapabilities": calendarCapabilities,
	})
	h.render(w, r, "calendar_view.html", data)
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
	allEvents, err = h.filterReadableCalendarEvents(r.Context(), user, cal, allEvents)
	if err != nil {
		http.Error(w, "failed to evaluate event access", http.StatusInternalServerError)
		return
	}

	// Filter events relevant to the requested month
	monthStart := time.Date(year, time.Month(month), 1, 0, 0, 0, 0, time.UTC)
	monthEnd := monthStart.AddDate(0, 1, 0).Add(-time.Second)
	relevantEvents := filterEventsForMonth(allEvents, monthStart, monthEnd)

	// Build JSON response
	var eventsJSONData []map[string]any
	for _, ev := range relevantEvents {
		eventsJSONData = append(eventsJSONData, calendarEventJSON(ev))
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(eventsJSONData); err != nil {
		http.Error(w, "failed to encode events", http.StatusInternalServerError)
	}
}

// defaultCalendarColors is a palette used when a calendar has no color set.
var defaultCalendarColors = []string{
	"#3b82f6", "#ef4444", "#10b981", "#f59e0b",
	"#8b5cf6", "#ec4899", "#06b6d4", "#f97316",
	"#6366f1", "#14b8a6",
}

func calendarColor(color *string, index int) string {
	if color != nil && *color != "" {
		return *color
	}
	return defaultCalendarColors[index%len(defaultCalendarColors)]
}

// ViewAllCalendars displays the aggregated all-calendars view.
func (h *Handler) ViewAllCalendars(w http.ResponseWriter, r *http.Request) {
	user, _ := auth.UserFromContext(r.Context())
	calendars, err := h.store.Calendars.ListAccessible(r.Context(), user.ID)
	if err != nil {
		http.Error(w, "failed to load calendars", http.StatusInternalServerError)
		return
	}

	type calendarMeta struct {
		ID    int64  `json:"id"`
		Name  string `json:"name"`
		Color string `json:"color"`
	}
	var metas []calendarMeta
	for i, cal := range calendars {
		metas = append(metas, calendarMeta{
			ID:    cal.ID,
			Name:  cal.Name,
			Color: calendarColor(cal.Calendar.Color, i),
		})
	}

	data := h.withFlash(r, map[string]any{
		"Title":     "All Calendars",
		"User":      user,
		"Calendars": metas,
	})
	h.render(w, r, "all_calendars_view.html", data)
}

// GetAllCalendarEventsJSON returns events across all accessible calendars for a given month.
func (h *Handler) GetAllCalendarEventsJSON(w http.ResponseWriter, r *http.Request) {
	year, _ := strconv.Atoi(r.URL.Query().Get("year"))
	month, _ := strconv.Atoi(r.URL.Query().Get("month"))
	if year == 0 || month < 1 || month > 12 {
		now := time.Now()
		year = now.Year()
		month = int(now.Month())
	}

	user, _ := auth.UserFromContext(r.Context())
	calendars, err := h.store.Calendars.ListAccessible(r.Context(), user.ID)
	if err != nil {
		http.Error(w, "failed to load calendars", http.StatusInternalServerError)
		return
	}

	monthStart := time.Date(year, time.Month(month), 1, 0, 0, 0, 0, time.UTC)
	monthEnd := monthStart.AddDate(0, 1, 0).Add(-time.Second)

	var result []map[string]any
	for i, cal := range calendars {
		allEvents, err := h.store.Events.ListForCalendar(r.Context(), cal.ID)
		if err != nil {
			continue
		}
		allEvents, err = h.filterReadableCalendarEvents(r.Context(), user, &cal, allEvents)
		if err != nil {
			http.Error(w, "failed to evaluate event access", http.StatusInternalServerError)
			return
		}

		color := calendarColor(cal.Calendar.Color, i)
		for _, ev := range filterEventsForMonth(allEvents, monthStart, monthEnd) {
			payload := calendarEventJSON(ev)
			payload["calendarId"] = cal.ID
			payload["calendarName"] = cal.Name
			payload["calendarColor"] = color
			result = append(result, payload)
		}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(result); err != nil {
		http.Error(w, "failed to encode events", http.StatusInternalServerError)
	}
}

// ExportCalendar downloads the readable events in a calendar as an iCalendar file.
func (h *Handler) ExportCalendar(w http.ResponseWriter, r *http.Request) {
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
	if err := h.requireCalendarPrivilege(r.Context(), user, cal, calendarACLResourcePath(calendarID), "read"); err != nil {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	events, err := h.store.Events.ListForCalendar(r.Context(), calendarID)
	if err != nil {
		http.Error(w, "failed to load events", http.StatusInternalServerError)
		return
	}
	events, err = h.filterReadableCalendarEvents(r.Context(), user, cal, events)
	if err != nil {
		http.Error(w, "failed to evaluate event access", http.StatusInternalServerError)
		return
	}

	filename := calendarExportFilename(cal.Name)
	w.Header().Set("Content-Type", "text/calendar; charset=utf-8")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, filename))
	_, _ = w.Write([]byte(buildCalendarExport(cal.Name, events)))
}

func calendarExportFilename(name string) string {
	name = strings.TrimSpace(strings.ToLower(name))
	var b strings.Builder
	lastDash := false
	for _, r := range name {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
			b.WriteRune(r)
			lastDash = false
		case r == '.' || r == '_':
			b.WriteRune(r)
			lastDash = false
		default:
			if !lastDash && b.Len() > 0 {
				b.WriteByte('-')
				lastDash = true
			}
		}
	}
	base := strings.Trim(b.String(), "-.")
	if base == "" {
		base = "calendar"
	}
	return base + ".ics"
}

func buildCalendarExport(name string, events []store.Event) string {
	var b strings.Builder
	b.WriteString("BEGIN:VCALENDAR\r\n")
	b.WriteString("VERSION:2.0\r\n")
	b.WriteString("PRODID:-//CalCard//Calendar Export//EN\r\n")
	b.WriteString("CALSCALE:GREGORIAN\r\n")
	if name = strings.TrimSpace(name); name != "" {
		b.WriteString("X-WR-CALNAME:")
		b.WriteString(utils.EscapeICalValue(name))
		b.WriteString("\r\n")
	}

	seenTimezones := make(map[string]struct{})
	for _, event := range events {
		for _, timezone := range rawICalComponents(event.RawICAL, "VTIMEZONE") {
			if _, ok := seenTimezones[timezone]; ok {
				continue
			}
			seenTimezones[timezone] = struct{}{}
			b.WriteString(timezone)
		}
	}
	for _, event := range events {
		for _, component := range rawICalComponents(event.RawICAL, "VEVENT") {
			b.WriteString(component)
		}
	}

	b.WriteString("END:VCALENDAR\r\n")
	return b.String()
}

func rawICalComponents(raw, componentName string) []string {
	componentName = strings.ToUpper(componentName)
	var components []string
	var current []string
	depth := 0

	for _, line := range utils.UnfoldLines(raw) {
		upperLine := strings.ToUpper(strings.TrimSpace(line))
		if depth == 0 {
			if upperLine == "BEGIN:"+componentName {
				current = []string{line}
				depth = 1
			}
			continue
		}

		current = append(current, line)
		if strings.HasPrefix(upperLine, "BEGIN:") {
			depth++
			continue
		}
		if strings.HasPrefix(upperLine, "END:") {
			depth--
			if depth == 0 {
				components = append(components, strings.Join(current, "\r\n")+"\r\n")
				current = nil
			}
		}
	}
	return components
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

	events, err := utils.ParseICSFile(string(icsData))
	if err != nil {
		h.redirect(w, r, fmt.Sprintf("/calendars/%d", calendarID), map[string]string{"error": fmt.Sprintf("invalid ICS file: %v", err)})
		return
	}

	imported := 0
	skipped := 0
	type pendingImport struct {
		uid          string
		resourceName string
		rawICAL      string
		etag         string
	}
	pending := make([]pendingImport, 0, len(events))
	for _, eventICAL := range events {
		uid := utils.ExtractUID(eventICAL)
		if uid == "" {
			uid = utils.GenerateUID()
			eventICAL = utils.EnsureUID(eventICAL, uid)
		}
		existing, err := h.store.Events.GetByUID(r.Context(), calendarID, uid)
		if err != nil {
			http.Error(w, "failed to load event", http.StatusInternalServerError)
			return
		}
		requiredPrivilege := "bind"
		resourceName := calendarEventResourceName(uid, existing)
		if existing != nil {
			requiredPrivilege = "write-content"
		}
		if err := h.requireCalendarPrivilege(r.Context(), user, cal, calendarEventResourcePath(calendarID, resourceName), requiredPrivilege); err != nil {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		pending = append(pending, pendingImport{
			uid:          uid,
			resourceName: resourceName,
			rawICAL:      eventICAL,
			etag:         utils.GenerateETag(eventICAL),
		})
	}

	for _, candidate := range pending {
		if _, err := h.store.Events.Upsert(r.Context(), store.Event{
			CalendarID:   calendarID,
			UID:          candidate.uid,
			ResourceName: candidate.resourceName,
			RawICAL:      candidate.rawICAL,
			ETag:         candidate.etag,
		}); err != nil {
			skipped++
			continue
		}
		imported++
	}

	if imported == 0 {
		h.redirect(w, r, fmt.Sprintf("/calendars/%d", calendarID), map[string]string{"error": fmt.Sprintf("failed to import events; skipped %d", skipped)})
		return
	}

	statusMsg := fmt.Sprintf("Imported %d event(s)", imported)
	if skipped > 0 {
		statusMsg = fmt.Sprintf("%s; skipped %d", statusMsg, skipped)
	}
	h.redirect(w, r, fmt.Sprintf("/calendars/%d", calendarID), map[string]string{
		"status": statusMsg,
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

	summary := strings.TrimSpace(r.FormValue("summary"))
	if summary == "" {
		h.redirect(w, r, fmt.Sprintf("/calendars/%d", calendarID), map[string]string{"error": "summary is required"})
		return
	}

	dtstart := strings.TrimSpace(r.FormValue("dtstart"))
	dtend := strings.TrimSpace(r.FormValue("dtend"))
	if dtstart == "" || dtend == "" {
		h.redirect(w, r, fmt.Sprintf("/calendars/%d", calendarID), map[string]string{"error": "start and end are required"})
		return
	}

	// Validate date format and range
	if err := validateEventDates(dtstart, dtend); err != nil {
		h.redirect(w, r, fmt.Sprintf("/calendars/%d", calendarID), map[string]string{"error": err.Error()})
		return
	}

	allDay := r.FormValue("all_day") == "on"
	location := strings.TrimSpace(r.FormValue("location"))
	description := strings.TrimSpace(r.FormValue("description"))
	opts := parseEventOptions(r)

	// Parse recurrence options
	recurrence := utils.ParseRecurrenceOptions(r)

	uid := utils.GenerateUID()
	if err := h.requireCalendarPrivilege(r.Context(), user, cal, calendarEventResourcePath(calendarID, uid), "bind"); err != nil {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	ical := utils.BuildEvent(uid, summary, dtstart, dtend, allDay, location, description, recurrence, opts)
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

	existing, err := h.store.Events.GetByUID(r.Context(), calendarID, uid)
	if err != nil {
		http.Error(w, "failed to load event", http.StatusInternalServerError)
		return
	}
	if existing == nil {
		http.Error(w, "event not found", http.StatusNotFound)
		return
	}
	if err := h.requireCalendarPrivilege(r.Context(), user, cal, calendarEventResourcePath(calendarID, calendarEventResourceName(uid, existing)), "write-content"); err != nil {
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
	if dtstart == "" || dtend == "" {
		h.redirect(w, r, fmt.Sprintf("/calendars/%d", calendarID), map[string]string{"error": "start and end are required"})
		return
	}

	// Validate date format and range
	if err := validateEventDates(dtstart, dtend); err != nil {
		h.redirect(w, r, fmt.Sprintf("/calendars/%d", calendarID), map[string]string{"error": err.Error()})
		return
	}

	allDay := r.FormValue("all_day") == "on"
	location := strings.TrimSpace(r.FormValue("location"))
	description := strings.TrimSpace(r.FormValue("description"))
	opts := parseEventOptions(r)

	// Parse recurrence options
	recurrence := utils.ParseRecurrenceOptions(r)

	ical := ""
	if editScope == "occurrence" && recurrenceID != "" && existing.RawICAL != "" {
		// Update only a single occurrence by replacing/adding a RECURRENCE-ID component.
		header, components, footer := utils.SplitComponents(existing.RawICAL)
		override := utils.BuildEventComponent(uid, summary, dtstart, dtend, allDay, location, description, nil, "", opts)
		if recLine, err := utils.FormatICalDateTime(recurrenceID, recurrenceAllDay, false, "RECURRENCE-ID", opts.Timezone); err == nil && recLine != "" {
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
		master := utils.BuildEventComponent(uid, summary, dtstart, dtend, allDay, location, description, recurrence, "", opts)
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
		if err := h.requireCalendarPrivilege(r.Context(), user, cal, calendarEventResourcePath(calendarID, calendarEventResourceName(uid, existing)), "write-content"); err != nil {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}

		timezone := strings.TrimSpace(r.FormValue("timezone"))
		exdateLine, err := utils.FormatICalDateTime(recurrenceID, recurrenceAllDay, false, "EXDATE", timezone)
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
		existing, err := h.store.Events.GetByUID(r.Context(), calendarID, uid)
		if err != nil {
			http.Error(w, "failed to load event", http.StatusInternalServerError)
			return
		}
		if existing == nil {
			http.Error(w, "event not found", http.StatusNotFound)
			return
		}
		if err := h.requireCalendarPrivilege(r.Context(), user, cal, calendarEventResourcePath(calendarID, calendarEventResourceName(uid, existing)), "unbind"); err != nil {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		if err := h.store.Events.DeleteByUID(r.Context(), calendarID, uid); err != nil {
			h.redirect(w, r, fmt.Sprintf("/calendars/%d", calendarID), map[string]string{"error": "failed to delete event"})
			return
		}
		h.redirect(w, r, fmt.Sprintf("/calendars/%d", calendarID), map[string]string{"status": "event_deleted"})
	}
}

func parseEventOptions(r *http.Request) *utils.EventOptions {
	timezone := strings.TrimSpace(r.FormValue("timezone"))
	status := strings.TrimSpace(r.FormValue("status"))
	class := strings.TrimSpace(r.FormValue("class"))
	transparency := strings.TrimSpace(r.FormValue("transparency"))
	url := strings.TrimSpace(r.FormValue("url"))
	organizer := strings.TrimSpace(r.FormValue("organizer"))

	categories := splitListField(r.FormValue("categories"))
	attendees := splitListField(r.FormValue("attendees"))
	attachments := splitListField(r.FormValue("attachments"))
	reminders := parseReminderMinutes(r.Form["reminder_minutes"])

	return &utils.EventOptions{
		Timezone:     timezone,
		URL:          url,
		Status:       status,
		Categories:   categories,
		Class:        class,
		Transparency: transparency,
		Organizer:    organizer,
		Attendees:    attendees,
		Attachments:  attachments,
		Reminders:    reminders,
	}
}

func splitListField(value string) []string {
	var out []string
	for _, part := range strings.FieldsFunc(value, func(r rune) bool { return r == '\n' || r == ',' }) {
		if v := strings.TrimSpace(part); v != "" {
			out = append(out, v)
		}
	}
	return out
}

func parseReminderMinutes(values []string) []int {
	var out []int
	for _, v := range values {
		v = strings.TrimSpace(v)
		if v == "" {
			continue
		}
		if n, err := strconv.Atoi(v); err == nil && n >= 0 {
			out = append(out, n)
		}
	}
	return out
}

// filterEventsForMonth returns events relevant to the given month range.
func filterEventsForMonth(allEvents []store.Event, monthStart, monthEnd time.Time) []store.Event {
	var relevant []store.Event
	for _, ev := range allEvents {
		hasRRule := strings.Contains(ev.RawICAL, "RRULE:")
		startDate := eventMonthFilterStart(ev)

		if hasRRule {
			if startDate != nil && !startDate.After(monthEnd) {
				hasEnded := false
				lines := utils.UnfoldLines(ev.RawICAL)
				for _, line := range lines {
					if strings.HasPrefix(line, "RRULE:") && strings.Contains(line, "UNTIL=") {
						parts := strings.Split(line, "UNTIL=")
						if len(parts) > 1 {
							untilStr := strings.Split(parts[1], ";")[0]
							untilStr = strings.Split(untilStr, "T")[0]
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
					relevant = append(relevant, ev)
				}
			}
		} else {
			endDate := eventMonthFilterEnd(ev, startDate)
			if startDate != nil && endDate != nil && !startDate.After(monthEnd) && !endDate.Before(monthStart) {
				relevant = append(relevant, ev)
			}
		}
	}
	return relevant
}

func eventMonthFilterStart(ev store.Event) *time.Time {
	start, _ := rawICalMonthFilterRange(ev.RawICAL)
	if start != nil {
		return start
	}
	if ev.DTStart == nil {
		return nil
	}
	normalized := time.Date(ev.DTStart.Year(), ev.DTStart.Month(), ev.DTStart.Day(), 0, 0, 0, 0, time.UTC)
	return &normalized
}

func eventMonthFilterEnd(ev store.Event, start *time.Time) *time.Time {
	_, end := rawICalMonthFilterRange(ev.RawICAL)
	if end != nil {
		if start != nil && end.Before(*start) {
			return start
		}
		return end
	}
	if ev.DTEnd != nil {
		normalized := time.Date(ev.DTEnd.Year(), ev.DTEnd.Month(), ev.DTEnd.Day(), 0, 0, 0, 0, time.UTC)
		if start != nil && normalized.Before(*start) {
			return start
		}
		return &normalized
	}
	return start
}

func rawICalMonthFilterRange(raw string) (*time.Time, *time.Time) {
	inEvent := false
	componentHasRecurrenceID := false
	var startFallback, endFallback *time.Time

	for _, line := range utils.UnfoldLines(raw) {
		switch line {
		case "BEGIN:VEVENT":
			inEvent = true
			componentHasRecurrenceID = false
			continue
		case "END:VEVENT":
			inEvent = false
			componentHasRecurrenceID = false
			continue
		}
		if !inEvent {
			continue
		}

		key, params, value, ok := parseICalProperty(line)
		if !ok {
			continue
		}

		switch key {
		case "RECURRENCE-ID":
			componentHasRecurrenceID = true
		case "DTSTART":
			start := rawICalCalendarDate(value, params, false)
			if start == nil {
				continue
			}
			if !componentHasRecurrenceID {
				if endFallback != nil {
					return start, endFallback
				}
				startFallback = start
				continue
			}
			if startFallback == nil {
				startFallback = start
			}
		case "DTEND":
			end := rawICalCalendarDate(value, params, true)
			if end == nil {
				continue
			}
			if !componentHasRecurrenceID {
				endFallback = end
				if startFallback != nil {
					return startFallback, endFallback
				}
				continue
			}
			if endFallback == nil {
				endFallback = end
			}
		}
	}

	return startFallback, endFallback
}

func rawICalCalendarDate(value string, params map[string]string, isEnd bool) *time.Time {
	value = strings.TrimSpace(value)
	if len(value) < 8 {
		return nil
	}
	datePart := value[:8]
	parsed, err := time.Parse("20060102", datePart)
	if err != nil {
		return nil
	}
	start := time.Date(parsed.Year(), parsed.Month(), parsed.Day(), 0, 0, 0, 0, time.UTC)
	if isEnd && (strings.EqualFold(params["VALUE"], "DATE") || len(value) == 8) {
		start = start.AddDate(0, 0, -1)
	}
	return &start
}

func calendarEventJSON(ev store.Event) map[string]any {
	payload := map[string]any{
		"uid":     ev.UID,
		"ical":    ev.RawICAL,
		"lastMod": ev.LastModified.Format(time.RFC3339),
	}

	parsed := parseCalendarEventMetadata(ev)
	parsed.addToPayload(payload)
	return payload
}

type calendarEventMetadata struct {
	UID                string
	Summary            string
	Description        string
	HTMLDescription    string
	Location           string
	DTStart            *time.Time
	DTEnd              *time.Time
	AllDay             bool
	Timezone           string
	Duration           string
	RRule              string
	RRuleParsed        map[string]string
	Exdates            []time.Time
	Status             string
	Categories         []string
	URL                string
	Class              string
	Transparency       string
	Organizer          string
	Attendees          []string
	Attachments        []string
	Reminders          []int
	RecurrenceID       *time.Time
	RecurrenceIDAllDay bool
	Overrides          []calendarEventMetadata
}

func (m calendarEventMetadata) addToPayload(payload map[string]any) {
	if m.UID != "" {
		payload["uid"] = m.UID
	}
	if m.Summary != "" {
		payload["summary"] = m.Summary
	}
	if m.Description != "" {
		payload["description"] = m.Description
	}
	if m.HTMLDescription != "" {
		payload["htmlDescription"] = m.HTMLDescription
	}
	if m.Location != "" {
		payload["location"] = m.Location
	}
	if m.DTStart != nil {
		payload["dtstart"] = m.DTStart.Format(time.RFC3339)
	}
	if m.DTEnd != nil {
		payload["dtend"] = m.DTEnd.Format(time.RFC3339)
	}
	payload["allDay"] = m.AllDay
	if m.Timezone != "" {
		payload["timezone"] = m.Timezone
	}
	if m.Duration != "" {
		payload["duration"] = m.Duration
	}
	if m.RRule != "" {
		payload["rrule"] = m.RRule
	}
	if len(m.RRuleParsed) > 0 {
		payload["rruleParsed"] = m.RRuleParsed
	}
	if len(m.Exdates) > 0 {
		payload["exdates"] = formatCalendarEventTimes(m.Exdates)
	}
	if m.Status != "" {
		payload["status"] = m.Status
	}
	if len(m.Categories) > 0 {
		payload["categories"] = m.Categories
	}
	if m.URL != "" {
		payload["url"] = m.URL
	}
	if m.Class != "" {
		payload["class"] = m.Class
	}
	if m.Transparency != "" {
		payload["transp"] = m.Transparency
	}
	if m.Organizer != "" {
		payload["organizer"] = m.Organizer
	}
	if len(m.Attendees) > 0 {
		payload["attendees"] = m.Attendees
	}
	if len(m.Attachments) > 0 {
		payload["attachments"] = m.Attachments
	}
	if len(m.Reminders) > 0 {
		payload["reminders"] = m.Reminders
	}
	if m.RecurrenceID != nil {
		payload["recurrenceId"] = m.RecurrenceID.Format(time.RFC3339)
		payload["recurrenceIdAllDay"] = m.RecurrenceIDAllDay
	}
	if len(m.Overrides) > 0 {
		overrides := make([]map[string]any, 0, len(m.Overrides))
		for _, override := range m.Overrides {
			overridePayload := make(map[string]any)
			override.addToPayload(overridePayload)
			overrides = append(overrides, overridePayload)
		}
		payload["overrides"] = overrides
	}
}

func parseCalendarEventMetadata(ev store.Event) calendarEventMetadata {
	components := parseCalendarEventComponents(ev.RawICAL)
	var master *calendarEventMetadata
	var overrides []calendarEventMetadata

	for i := range components {
		component := components[i]
		if component.UID == "" {
			component.UID = ev.UID
		}
		if component.RecurrenceID != nil {
			overrides = append(overrides, component)
			continue
		}
		if master == nil {
			master = &component
		}
	}
	if master == nil {
		if len(components) > 0 {
			component := components[0]
			master = &component
		} else {
			master = &calendarEventMetadata{}
		}
	}

	if master.UID == "" {
		master.UID = ev.UID
	}
	if ev.Summary != nil && master.Summary == "" {
		master.Summary = *ev.Summary
	}
	if ev.DTStart != nil {
		master.DTStart = ev.DTStart
	}
	if ev.DTEnd != nil {
		master.DTEnd = ev.DTEnd
	}
	master.AllDay = ev.AllDay
	master.Overrides = overrides
	return *master
}

func parseCalendarEventComponents(raw string) []calendarEventMetadata {
	var components []calendarEventMetadata
	var current []string
	inEvent := false

	for _, line := range utils.UnfoldLines(raw) {
		switch {
		case strings.EqualFold(line, "BEGIN:VEVENT"):
			inEvent = true
			current = nil
		case strings.EqualFold(line, "END:VEVENT"):
			if inEvent {
				components = append(components, parseCalendarEventComponent(current))
			}
			inEvent = false
			current = nil
		case inEvent:
			current = append(current, line)
		}
	}

	return components
}

func parseCalendarEventComponent(lines []string) calendarEventMetadata {
	var event calendarEventMetadata
	inAlarm := false

	for _, line := range lines {
		key, params, value, ok := parseICalProperty(line)
		if !ok {
			continue
		}
		switch key {
		case "BEGIN":
			if strings.EqualFold(value, "VALARM") {
				inAlarm = true
			}
			continue
		case "END":
			if strings.EqualFold(value, "VALARM") {
				inAlarm = false
			}
			continue
		}

		if inAlarm {
			if key == "TRIGGER" {
				if minutes, ok := parseTriggerMinutes(value); ok {
					event.Reminders = append(event.Reminders, minutes)
				}
			}
			continue
		}

		switch key {
		case "UID":
			event.UID = value
		case "SUMMARY":
			event.Summary = unescapeICalText(value)
		case "DESCRIPTION":
			event.Description = unescapeICalText(value)
		case "RECURRENCE-ID":
			if t, allDay := parseCalendarEventDate(value, params); t != nil {
				event.RecurrenceID = t
				event.RecurrenceIDAllDay = allDay
			}
		case "X-ALT-DESC":
			if strings.EqualFold(params["FMTTYPE"], "text/html") {
				event.HTMLDescription = value
			}
		case "LOCATION":
			event.Location = unescapeICalText(value)
		case "DTSTART":
			if t, allDay := parseCalendarEventDate(value, params); t != nil {
				event.DTStart = t
				event.AllDay = allDay
			}
			if tzid := params["TZID"]; tzid != "" {
				event.Timezone = tzid
			}
		case "DTEND":
			if t, _ := parseCalendarEventDate(value, params); t != nil {
				event.DTEnd = t
			}
			if event.Timezone == "" {
				event.Timezone = params["TZID"]
			}
		case "DURATION":
			event.Duration = value
		case "RRULE":
			event.RRule = value
			event.RRuleParsed = parseRRuleParts(value)
		case "EXDATE":
			event.Exdates = append(event.Exdates, parseCalendarEventDateList(value, params)...)
		case "STATUS":
			event.Status = value
		case "CATEGORIES":
			event.Categories = splitICalTextList(value)
		case "URL":
			event.URL = value
		case "CLASS":
			event.Class = value
		case "TRANSP":
			event.Transparency = value
		case "ORGANIZER":
			event.Organizer = formatICalEmailValue(value, params)
		case "ATTENDEE":
			event.Attendees = append(event.Attendees, formatICalEmailValue(value, params))
		case "ATTACH":
			event.Attachments = append(event.Attachments, value)
		}
	}

	return event
}

func parseICalProperty(line string) (string, map[string]string, string, bool) {
	colonIdx := strings.Index(line, ":")
	if colonIdx == -1 {
		return "", nil, "", false
	}

	keyPart := line[:colonIdx]
	value := line[colonIdx+1:]
	keyParts := splitICalParameterParts(keyPart)
	key := strings.ToUpper(strings.TrimSpace(keyParts[0]))
	params := make(map[string]string)
	for _, part := range keyParts[1:] {
		name, val, ok := strings.Cut(part, "=")
		if !ok {
			continue
		}
		params[strings.ToUpper(strings.TrimSpace(name))] = strings.Trim(strings.TrimSpace(val), `"`)
	}
	return key, params, value, key != ""
}

func splitICalParameterParts(value string) []string {
	var parts []string
	var current strings.Builder
	inQuote := false
	for _, r := range value {
		switch r {
		case '"':
			inQuote = !inQuote
			current.WriteRune(r)
		case ';':
			if inQuote {
				current.WriteRune(r)
				continue
			}
			parts = append(parts, current.String())
			current.Reset()
		default:
			current.WriteRune(r)
		}
	}
	parts = append(parts, current.String())
	return parts
}

func parseCalendarEventDateList(value string, params map[string]string) []time.Time {
	values := strings.Split(value, ",")
	dates := make([]time.Time, 0, len(values))
	for _, v := range values {
		if t, _ := parseCalendarEventDate(v, params); t != nil {
			dates = append(dates, *t)
		}
	}
	return dates
}

func parseCalendarEventDate(value string, params map[string]string) (*time.Time, bool) {
	value = strings.TrimSpace(value)
	allDay := strings.EqualFold(params["VALUE"], "DATE") || (len(value) == 8 && !strings.Contains(value, "T"))
	if allDay {
		if len(value) < 8 {
			return nil, false
		}
		t, err := time.Parse("20060102", value[:8])
		if err != nil {
			return nil, false
		}
		return &t, true
	}

	for _, layout := range []string{"20060102T150405Z", "20060102T150405-0700", "20060102T150405-07:00"} {
		if t, err := time.Parse(layout, value); err == nil {
			utc := t.UTC()
			return &utc, false
		}
	}

	tzid := params["TZID"]
	loc := time.UTC
	if tzid != "" {
		if loaded, err := loadCalendarEventLocation(tzid); err == nil {
			loc = loaded
		}
	}
	if t, err := time.ParseInLocation("20060102T150405", strings.TrimSuffix(value, "Z"), loc); err == nil {
		utc := t.UTC()
		return &utc, false
	}

	return nil, false
}

func loadCalendarEventLocation(tzid string) (*time.Location, error) {
	switch tzid {
	case "US/Central":
		tzid = "America/Chicago"
	case "US/Eastern":
		tzid = "America/New_York"
	case "US/Mountain":
		tzid = "America/Denver"
	case "US/Pacific":
		tzid = "America/Los_Angeles"
	}
	return time.LoadLocation(tzid)
}

func parseRRuleParts(value string) map[string]string {
	parts := make(map[string]string)
	for _, part := range strings.Split(value, ";") {
		key, val, ok := strings.Cut(part, "=")
		if ok {
			parts[strings.ToUpper(strings.TrimSpace(key))] = strings.TrimSpace(val)
		}
	}
	return parts
}

func parseTriggerMinutes(value string) (int, bool) {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0, false
	}
	if value[0] == '+' {
		return 0, false
	}
	if value[0] == '-' {
		value = value[1:]
	}
	if !strings.HasPrefix(value, "P") {
		return 0, false
	}

	days, hours, minutes, seconds := 0, 0, 0, 0
	timePart := false
	var digits strings.Builder
	flush := func(unit rune) bool {
		if digits.Len() == 0 {
			return false
		}
		n, err := strconv.Atoi(digits.String())
		if err != nil {
			return false
		}
		digits.Reset()
		switch unit {
		case 'D':
			days = n
		case 'H':
			hours = n
		case 'M':
			if timePart {
				minutes = n
			}
		case 'S':
			seconds = n
		}
		return true
	}

	for _, r := range value[1:] {
		switch {
		case r >= '0' && r <= '9':
			digits.WriteRune(r)
		case r == 'T':
			timePart = true
		case r == 'D' || r == 'H' || r == 'M' || r == 'S':
			if !flush(r) {
				return 0, false
			}
		default:
			return 0, false
		}
	}

	return days*1440 + hours*60 + minutes + (seconds+30)/60, true
}

func unescapeICalText(value string) string {
	value = strings.ReplaceAll(value, "\\n", "\n")
	value = strings.ReplaceAll(value, "\\N", "\n")
	value = strings.ReplaceAll(value, "\\,", ",")
	value = strings.ReplaceAll(value, "\\;", ";")
	value = strings.ReplaceAll(value, "\\\\", "\\")
	return value
}

func splitICalTextList(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(unescapeICalText(part))
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func formatICalEmailValue(value string, params map[string]string) string {
	email := strings.TrimSpace(value)
	if strings.HasPrefix(strings.ToLower(email), "mailto:") {
		email = email[7:]
	}
	name := params["CN"]
	if email == "" {
		return ""
	}
	if name == "" {
		return email
	}
	return fmt.Sprintf("%s <%s>", name, email)
}

func formatCalendarEventTimes(times []time.Time) []string {
	formatted := make([]string, 0, len(times))
	for _, t := range times {
		formatted = append(formatted, t.Format(time.RFC3339))
	}
	return formatted
}

// validateEventDates validates that the date strings are parseable and end is after start
func validateEventDates(dtstart, dtend string) error {
	// Try parsing as datetime-local format (YYYY-MM-DDTHH:MM)
	layouts := []string{
		"2006-01-02T15:04",          // datetime-local
		"2006-01-02T15:04:05",       // datetime-local with seconds
		"2006-01-02T15:04:05Z07:00", // RFC3339
		"2006-01-02",                // date only
	}

	var startTime, endTime time.Time
	var startErr, endErr error

	// Try parsing start time
	for _, layout := range layouts {
		startTime, startErr = time.Parse(layout, dtstart)
		if startErr == nil {
			break
		}
	}
	if startErr != nil {
		return fmt.Errorf("invalid start date format")
	}

	// Try parsing end time
	for _, layout := range layouts {
		endTime, endErr = time.Parse(layout, dtend)
		if endErr == nil {
			break
		}
	}
	if endErr != nil {
		return fmt.Errorf("invalid end date format")
	}

	// Validate that end is after start
	if !endTime.After(startTime) {
		return fmt.Errorf("end date must be after start date")
	}

	return nil
}
