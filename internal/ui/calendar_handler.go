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
			result = append(result, map[string]any{
				"uid":           ev.UID,
				"ical":          ev.RawICAL,
				"lastMod":       ev.LastModified.Format(time.RFC3339),
				"calendarId":    cal.ID,
				"calendarName":  cal.Name,
				"calendarColor": color,
			})
		}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(result); err != nil {
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

		if hasRRule {
			if ev.DTStart != nil && !ev.DTStart.After(monthEnd) {
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
			if ev.DTStart != nil {
				evStart := time.Date(ev.DTStart.Year(), ev.DTStart.Month(), ev.DTStart.Day(), 0, 0, 0, 0, time.UTC)
				if !evStart.Before(monthStart) && !evStart.After(monthEnd) {
					relevant = append(relevant, ev)
				}
			}
		}
	}
	return relevant
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
