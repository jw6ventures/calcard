package ui

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jw6ventures/calcard/internal/acl"
	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/calobject"
	"github.com/jw6ventures/calcard/internal/ical"
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
	h.redirect(w, r, "/calendars", map[string]string{"status": "calendar_created"})
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
	h.redirect(w, r, "/calendars", map[string]string{"status": "calendar_renamed"})
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
	h.redirect(w, r, "/calendars", map[string]string{"status": "calendar_deleted"})
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

	h.redirect(w, r, "/calendars", map[string]string{"status": "calendar_shared"})
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

	h.redirect(w, r, "/calendars", map[string]string{"status": "calendar_updated"})
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

// calendarShareManagedPrivileges are the collection grants the sharing UI owns,
// and so the only ones it takes back when a share is revoked.
var calendarShareManagedPrivileges = []string{"read", "read-free-busy", "write"}

func calendarShareManagedPrivilege(privilege string) bool {
	return slices.Contains(calendarShareManagedPrivileges, privilege)
}

func calendarShareVisiblePrivilege(privilege string) bool {
	switch privilege {
	case "read", "read-free-busy", "write", "write-content", "write-properties", "bind", "unbind", "all":
		return true
	default:
		return false
	}
}

func calendarSharePresetEntries(calendarID, userID int64, editor bool, position int) []store.ACLEntry {
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
			Position:      position,
		})
	}
	return entries
}

// calendarApplicablePrincipalHrefs returns the principal hrefs whose ACL entries
// apply to user when deciding calendar access. It is the single source of truth
// shared by the per-request ACL prefetch sweep and the in-memory privilege
// evaluator, so the two can never diverge.
func calendarApplicablePrincipalHrefs(user *store.User) []string {
	return acl.PrincipalHrefs(user)
}

// decideCalendarPrivilege evaluates the resource's ordered ACEs before falling
// back to the collection's ordered ACEs. Alternate resource spellings are one
// resource identity, so their entries are merged by ACE position first.
func decideCalendarPrivilege(user *store.User, cal *store.CalendarAccess, resourcePath, privilege string, lookup func(candidate string) ([]store.ACLEntry, error)) (bool, error) {
	if cal == nil || user == nil {
		return false, nil
	}
	if cal.UserID == user.ID {
		return true, nil
	}

	applicablePrincipals := acl.ApplicablePrincipals(user)
	collectionPath := calendarACLResourcePath(cal.ID)
	if path.Clean(resourcePath) != collectionPath {
		var resourceEntries []store.ACLEntry
		for _, candidate := range calendarACLLookupPaths(resourcePath) {
			entries, err := lookup(candidate)
			if err != nil {
				return false, err
			}
			resourceEntries = append(resourceEntries, entries...)
		}
		acl.SortEntries(resourceEntries)
		if granted, decided := acl.DecisionForPrivilege(resourceEntries, applicablePrincipals, privilege); decided {
			return granted, nil
		}
	}

	entries, err := lookup(collectionPath)
	if err != nil {
		return false, err
	}
	acl.SortEntries(entries)
	granted, _ := acl.DecisionForPrivilege(entries, applicablePrincipals, privilege)
	return granted, nil
}

// uiCalendarComponent is the component type every calendar object resource the
// UI writes carries. The event forms build a VEVENT and nothing else, so the
// destination's CALDAV:supported-calendar-component-set is judged against it.
const uiCalendarComponent = "VEVENT"

// calendarAcceptsUIWrite reports whether a collection admits what the UI is
// about to store in it. RFC 4791 Section 5.2.3 makes
// CALDAV:supported-calendar-component-set a restriction on the collection's
// contents, which the DAV PUT path enforces; a UI write that ignored it would
// leave the collection holding content it advertises it does not accept, and
// clients reading that property would never ask for it.
func calendarAcceptsUIWrite(cal *store.CalendarAccess) bool {
	return cal != nil && cal.AcceptsComponent(uiCalendarComponent)
}

// validatedEvent puts calendar data the UI built or imported through the
// validator a DAV PUT into cal applies, so the UI stores nothing a CalDAV client
// could not have written, and returns the event to store: the octets the
// validator accepted, under the UID they carry, indexed by the metadata the
// validator derived.
func (h *Handler) validatedEvent(cal *store.CalendarAccess, resourceName, raw string) (store.Event, error) {
	object, err := h.validateCalendarObject(raw, cal)
	if err != nil {
		return store.Event{}, err
	}
	return store.Event{
		CalendarID:    cal.ID,
		UID:           object.UID,
		ResourceName:  resourceName,
		RawICAL:       object.Body,
		ETag:          utils.GenerateETag(object.Body),
		WriteMetadata: &object.Metadata,
	}, nil
}

// occurrenceRemovedEvent is the event to store once an occurrence is removed
// from existing, leaving raw. Data stored before the validator existed may fail
// it, and the editor refuses every change to such a series; removing an
// occurrence leaves that data no worse than it was, so it is still allowed,
// as the only way to drop one short of deleting the series. That result is
// stored as it is, and the store derives its metadata.
func (h *Handler) occurrenceRemovedEvent(cal *store.CalendarAccess, existing *store.Event, resourceName, raw string) (store.Event, error) {
	event, err := h.validatedEvent(cal, resourceName, raw)
	if err == nil {
		return event, nil
	}
	if _, storedErr := h.validateCalendarObject(existing.RawICAL, cal); storedErr == nil {
		return store.Event{}, err
	}
	return store.Event{
		CalendarID:   cal.ID,
		UID:          existing.UID,
		ResourceName: resourceName,
		RawICAL:      raw,
		ETag:         utils.GenerateETag(raw),
	}, nil
}

// invalidEventMessage is the flash message for calendar data the validator
// refused: what did not happen, then why, in words for the user rather than
// the RFC 4791 precondition names a DAV client is answered with.
func invalidEventMessage(action string, err error) string {
	const fallback = "the server could not accept its calendar data"
	var refusal *calobject.Error
	if !errors.As(err, &refusal) || len(refusal.Conditions) == 0 {
		return action + ": " + fallback
	}
	reasons := make([]string, 0, len(refusal.Conditions))
	for _, condition := range refusal.Conditions {
		reason, ok := calendarObjectRefusalReasons[condition]
		if !ok {
			reason = fallback
		}
		if !slices.Contains(reasons, reason) {
			reasons = append(reasons, reason)
		}
	}
	return action + ": " + strings.Join(reasons, "; ")
}

var calendarObjectRefusalReasons = map[string]string{
	"valid-calendar-data":            "its calendar data is not valid iCalendar",
	"valid-calendar-object-resource": "it is not a single event with one UID, or it uses a time zone it does not define",
	"supported-calendar-component":   "this calendar does not accept that kind of item",
	"max-resource-size":              "it is larger than this server accepts",
	"min-date-time":                  "it has a date earlier than this server accepts",
	"max-date-time":                  "it has a date later than this server accepts",
	"max-instances":                  "it repeats more times than this server allows",
	"max-attendees-per-instance":     "it has more attendees than this server allows",
}

const eventNotSaved = "the event could not be saved"

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

	allowed, err := decideCalendarPrivilege(user, cal, resourcePath, privilege, func(candidate string) ([]store.ACLEntry, error) {
		return h.store.ACLEntries.ListByResource(ctx, candidate)
	})
	if err != nil {
		return err
	}
	if allowed {
		return nil
	}
	return store.ErrNotFound
}

func (h *Handler) hasCalendarPrivilege(ctx context.Context, user *store.User, cal *store.CalendarAccess, resourcePath, privilege string) (bool, error) {
	err := h.requireCalendarPrivilege(ctx, user, cal, resourcePath, privilege)
	if err == nil {
		return true, nil
	}
	if err == store.ErrNotFound {
		return false, nil
	}
	return false, err
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

// prefetchCalendarACLEntries loads, in one scoped query, the ACL entries that
// apply to user and target the given events' resource candidate paths or the
// calendar's collection path, keyed by exact resource path. The query is bounded
// to exactly the paths the decider will consult, so it does not scan or transfer
// unrelated server-wide ACL state. This mirrors the DAV batching in
// internal/dav/calendar_access.go (prefetchCalendarACLEntries / prefetchACLEntries).
func (h *Handler) prefetchCalendarACLEntries(ctx context.Context, user *store.User, calendarID int64, events []store.Event) (map[string][]store.ACLEntry, error) {
	collectionPath := calendarACLResourcePath(calendarID)
	relevantPaths := make([]string, 0, 1+2*len(events))
	seen := make(map[string]struct{}, 1+2*len(events))
	addPath := func(candidate string) {
		if _, ok := seen[candidate]; ok {
			return
		}
		seen[candidate] = struct{}{}
		relevantPaths = append(relevantPaths, candidate)
	}
	addPath(collectionPath)
	for _, event := range events {
		resourcePath := calendarEventResourcePath(calendarID, calendarEventResourceName(event.UID, &event))
		for _, candidate := range calendarACLLookupPaths(resourcePath) {
			addPath(candidate)
		}
	}

	entries, err := h.store.ACLEntries.ListByResourcesAndPrincipals(ctx, relevantPaths, calendarApplicablePrincipalHrefs(user))
	if err != nil {
		return nil, err
	}
	entriesByPath := make(map[string][]store.ACLEntry, len(relevantPaths))
	for _, entry := range entries {
		entriesByPath[entry.ResourcePath] = append(entriesByPath[entry.ResourcePath], entry)
	}
	for resourcePath := range entriesByPath {
		acl.SortEntries(entriesByPath[resourcePath])
	}
	return entriesByPath, nil
}

// calendarPrivilegeDecider prefetches the ACL state for events once (for a shared
// calendar backed by an ACL store) and returns a closure that decides any
// privilege for those events' resources without further repository calls. Its
// branch structure mirrors requireCalendarPrivilege exactly: owners are always
// allowed, the effective-privilege fallback applies only when no ACL store is
// configured, and otherwise decisions come from the prefetched entries.
func (h *Handler) calendarPrivilegeDecider(ctx context.Context, user *store.User, cal *store.CalendarAccess, events []store.Event) (func(resourcePath, privilege string) bool, error) {
	if cal == nil || user == nil {
		return func(string, string) bool { return false }, nil
	}
	if cal.UserID == user.ID {
		return func(string, string) bool { return true }, nil
	}
	if h == nil || h.store == nil || h.store.ACLEntries == nil {
		privileges := cal.EffectivePrivileges()
		return func(_, privilege string) bool { return privileges.Allows(privilege) }, nil
	}

	entriesByPath, err := h.prefetchCalendarACLEntries(ctx, user, cal.ID, events)
	if err != nil {
		return nil, err
	}
	lookup := func(candidate string) ([]store.ACLEntry, error) {
		return entriesByPath[candidate], nil
	}
	return func(resourcePath, privilege string) bool {
		allowed, _ := decideCalendarPrivilege(user, cal, resourcePath, privilege, lookup)
		return allowed
	}, nil
}

// filterReadableCalendarEventsWith keeps only the events readable under decide,
// a privilege closure the caller has already built once for cal.
func filterReadableCalendarEventsWith(decide func(resourcePath, privilege string) bool, cal *store.CalendarAccess, events []store.Event) []store.Event {
	visible := make([]store.Event, 0, len(events))
	for _, event := range events {
		resourcePath := calendarEventResourcePath(cal.ID, calendarEventResourceName(event.UID, &event))
		if decide(resourcePath, "read") {
			visible = append(visible, event)
		}
	}
	return visible
}

func (h *Handler) filterReadableCalendarEvents(ctx context.Context, user *store.User, cal *store.CalendarAccess, events []store.Event) ([]store.Event, error) {
	decide, err := h.calendarPrivilegeDecider(ctx, user, cal, events)
	if err != nil {
		return nil, err
	}
	return filterReadableCalendarEventsWith(decide, cal, events), nil
}

func (h *Handler) calendarShareViews(ctx context.Context, calendarID int64, userMap map[int64]store.User) ([]calendarShareView, error) {
	resourcePath := calendarACLResourcePath(calendarID)
	entries, err := h.store.ACLEntries.ListByResource(ctx, resourcePath)
	if err != nil {
		return nil, err
	}
	acl.SortEntries(entries)

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
		if createdAt[userID].IsZero() || entry.CreatedAt.Before(createdAt[userID]) {
			createdAt[userID] = entry.CreatedAt
		}
	}

	shares := make([]calendarShareView, 0, len(createdAt))
	for userID := range createdAt {
		u, ok := userMap[userID]
		if !ok {
			continue
		}
		visible, editor := effectiveCalendarShareAccess(entries, userID)
		if !visible {
			continue
		}
		shares = append(shares, calendarShareView{
			User:      u,
			Editor:    editor,
			CreatedAt: createdAt[userID],
		})
	}
	sort.Slice(shares, func(i, j int) bool {
		return shares[i].User.ReferenceName() < shares[j].User.ReferenceName()
	})
	return shares, nil
}

func effectiveCalendarShareAccess(entries []store.ACLEntry, userID int64) (bool, bool) {
	applicable := acl.ApplicablePrincipals(&store.User{ID: userID})
	visible := false
	for _, privilege := range []string{"read", "read-free-busy"} {
		granted, _ := acl.DecisionForPrivilege(entries, applicable, privilege)
		visible = visible || granted
	}
	editor := false
	for _, privilege := range []string{"write-content", "write-properties", "bind", "unbind"} {
		granted, _ := acl.DecisionForPrivilege(entries, applicable, privilege)
		editor = editor || granted
	}
	return visible || editor, editor
}

func (h *Handler) setCalendarShare(ctx context.Context, calendarID, userID int64, editor bool) error {
	resourcePath := calendarACLResourcePath(calendarID)
	principalHref := calendarSharePrincipalHref(userID)
	return h.store.ACLEntries.UpdateACL(ctx, resourcePath, func(entries []store.ACLEntry) ([]store.ACLEntry, error) {
		filtered := make([]store.ACLEntry, 0, len(entries))
		sharePosition := -1
		maxPosition := -1
		for _, entry := range entries {
			if entry.Position > maxPosition {
				maxPosition = entry.Position
			}
			if acl.NormalizePrincipalHref(entry.PrincipalHref) == principalHref && entry.IsGrant && calendarShareManagedPrivilege(entry.Privilege) {
				if sharePosition == -1 || entry.Position < sharePosition {
					sharePosition = entry.Position
				}
				continue
			}
			filtered = append(filtered, entry)
		}
		if sharePosition == -1 {
			sharePosition = maxPosition + 1
		}
		return append(filtered, calendarSharePresetEntries(calendarID, userID, editor, sharePosition)...), nil
	})
}

// removeCalendarShare revokes a principal's share of one calendar. It reaches
// further than the collection grants it writes: a grant on one event is
// evaluated ahead of the calendar's, so one left in place would keep that event
// readable to a principal the calendar no longer names, and a member grant
// records no origin to tell a share's leftovers from one the owner set through
// the ACL method on that event. Every grant the principal holds anywhere in the
// calendar therefore goes. Only grants are removed, so this never widens
// access and needs no precondition, whoever asks for it.
func (h *Handler) removeCalendarShare(ctx context.Context, calendarID, userID int64) error {
	return h.store.ACLEntries.RevokePrincipalGrants(ctx, calendarACLResourcePath(calendarID), calendarSharePrincipalHref(userID), calendarShareManagedPrivileges)
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
		"CalendarColor":        calendarColor(cal.Color, 0),
		"CalendarCapabilities": calendarCapabilities,
	})
	h.render(w, r, "calendar_view.html", data)
}

// GetCalendarEventsJSON returns events for a calendar in JSON format. It accepts either an
// explicit start/end date range (day/week views) or a year/month pair (month/list views).
func (h *Handler) GetCalendarEventsJSON(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		http.Error(w, "invalid calendar id", http.StatusBadRequest)
		return
	}

	rangeStart, rangeEndExclusive, err := calendarEventQueryRange(r, time.Now())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
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

	// Filter events relevant to the requested range (end is inclusive here).
	rangeEnd := rangeEndExclusive.Add(-time.Nanosecond)
	relevantEvents := filterEventsForMonth(allEvents, rangeStart, rangeEnd)

	// Build JSON response
	viewStart, viewEnd := eventViewWindow(rangeStart, rangeEndExclusive)
	eventsJSONData := make([]map[string]any, 0, len(relevantEvents))
	for _, ev := range relevantEvents {
		payload := unfoldedEventJSON(ev, viewStart, viewEnd, h.timezoneOnsets)
		h.addSeriesInstances(payload, ev, viewStart, viewEnd)
		eventsJSONData = append(eventsJSONData, payload)
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
		ID        int64  `json:"id"`
		Name      string `json:"name"`
		Color     string `json:"color"`
		CanCreate bool   `json:"canCreate"`
	}
	var metas []calendarMeta
	for i, cal := range calendars {
		canCreate, err := h.hasCalendarPrivilege(
			r.Context(),
			user,
			&cal,
			calendarACLResourcePath(cal.ID),
			"bind",
		)
		if err != nil {
			http.Error(w, "failed to evaluate calendar access", http.StatusInternalServerError)
			return
		}
		metas = append(metas, calendarMeta{
			ID:   cal.ID,
			Name: cal.Name,
			// Bind alone does not make a collection somewhere an event may go:
			// one restricted to VTODO is writable and still refuses it. The
			// page gates the new-event button and the move destinations on
			// this, so both have to mean the same thing the handlers enforce.
			Color:     calendarColor(cal.Calendar.Color, i),
			CanCreate: canCreate && calendarAcceptsUIWrite(&cal),
		})
	}

	data := h.withFlash(r, map[string]any{
		"Title":     "All Calendars",
		"User":      user,
		"Calendars": metas,
	})
	h.render(w, r, "all_calendars_view.html", data)
}

const maxAllCalendarRangeDays = 62

// eventViewWindow is the span of instants a view showing the dates [start,
// endExclusive) can show, each given as its UTC midnight: the browser shows
// its own local dates, which begin up to maxUTCOffset either side of the UTC
// midnight of the same date.
func eventViewWindow(start, endExclusive time.Time) (time.Time, time.Time) {
	return start.Add(-maxUTCOffset), endExclusive.Add(maxUTCOffset)
}

// calendarEventInstance is one instance of a series in the view model, as
// calobject.Instance describes it.
type calendarEventInstance struct {
	RecurrenceID string `json:"recurrenceId"`
	Start        string `json:"start"`
	End          string `json:"end"`
	Override     string `json:"override,omitempty"`
}

// addSeriesInstances adds to payload, the view model of unfolded, the
// instances of its series from viewStart to viewEnd, as CalDAV expands them:
// "instances", which the browser places the series by, and
// "instancesComplete", false when the range holds more than it lists. An
// event whose master does not recur, or whose recurrence the server cannot
// expand, gets neither, and stands for itself.
func (h *Handler) addSeriesInstances(payload map[string]any, unfolded unfoldedEvent, viewStart, viewEnd time.Time) {
	if !unfolded.series.recurs() {
		return
	}
	expansion := h.expandEvent(unfolded.event.RawICAL, viewStart, viewEnd)
	if !expansion.Recurs {
		return
	}
	instances := make([]calendarEventInstance, 0, len(expansion.Instances))
	for _, instance := range expansion.Instances {
		listed := calendarEventInstance{
			RecurrenceID: instance.RecurrenceID.UTC().Format(time.RFC3339),
			Start:        instance.Start.UTC().Format(time.RFC3339),
			End:          instance.End.UTC().Format(time.RFC3339),
		}
		if !instance.Override.IsZero() {
			listed.Override = instance.Override.UTC().Format(time.RFC3339)
		}
		instances = append(instances, listed)
	}
	payload["instances"] = instances
	payload["instancesComplete"] = expansion.Complete
}

// calendarEventQueryRange resolves the [start, endExclusive) window an events.json
// request targets. It accepts an explicit start/end date range (used by the day/week
// views) and falls back to a whole-month window derived from year/month.
func calendarEventQueryRange(r *http.Request, now time.Time) (time.Time, time.Time, error) {
	startValue := strings.TrimSpace(r.URL.Query().Get("start"))
	endValue := strings.TrimSpace(r.URL.Query().Get("end"))
	if startValue != "" || endValue != "" {
		if startValue == "" || endValue == "" {
			return time.Time{}, time.Time{}, fmt.Errorf("start and end are required")
		}
		start, err := time.Parse("2006-01-02", startValue)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("invalid start date")
		}
		endExclusive, err := time.Parse("2006-01-02", endValue)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("invalid end date")
		}
		if !endExclusive.After(start) || endExclusive.Sub(start) > maxAllCalendarRangeDays*24*time.Hour {
			return time.Time{}, time.Time{}, fmt.Errorf("invalid date range")
		}
		return start, endExclusive, nil
	}

	year, _ := strconv.Atoi(r.URL.Query().Get("year"))
	month, _ := strconv.Atoi(r.URL.Query().Get("month"))
	if year == 0 || month < 1 || month > 12 {
		year = now.Year()
		month = int(now.Month())
	}
	start := time.Date(year, time.Month(month), 1, 0, 0, 0, 0, time.UTC)
	return start, start.AddDate(0, 1, 0), nil
}

// GetAllCalendarEventsJSON returns events across all accessible calendars for a date range.
func (h *Handler) GetAllCalendarEventsJSON(w http.ResponseWriter, r *http.Request) {
	rangeStart, rangeEndExclusive, err := calendarEventQueryRange(r, time.Now())
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	user, _ := auth.UserFromContext(r.Context())
	calendars, err := h.store.Calendars.ListAccessible(r.Context(), user.ID)
	if err != nil {
		http.Error(w, "failed to load calendars", http.StatusInternalServerError)
		return
	}

	rangeEnd := rangeEndExclusive.Add(-time.Nanosecond)
	viewStart, viewEnd := eventViewWindow(rangeStart, rangeEndExclusive)

	var result = make([]map[string]any, 0)
	for i, cal := range calendars {
		allEvents, err := h.store.Events.ListForCalendar(r.Context(), cal.ID)
		if err != nil {
			continue
		}

		// Restrict to the requested window before any ACL work so large
		// calendars only pay to resolve access for events that can appear in
		// the response, not their entire history.
		unfolded := filterEventsForMonth(allEvents, rangeStart, rangeEnd)
		if len(unfolded) == 0 {
			continue
		}
		windowed := unfoldedStoreEvents(unfolded)

		// One scoped ACL prefetch per calendar then resolves read,
		// write-content, and unbind for every windowed event in memory.
		decide, err := h.calendarPrivilegeDecider(r.Context(), user, &cal, windowed)
		if err != nil {
			http.Error(w, "failed to evaluate event access", http.StatusInternalServerError)
			return
		}

		color := calendarColor(cal.Calendar.Color, i)
		for _, unfoldedEv := range unfolded {
			ev := unfoldedEv.event
			resourcePath := calendarEventResourcePath(cal.ID, calendarEventResourceName(ev.UID, &ev))
			if !decide(resourcePath, "read") {
				continue
			}

			payload := unfoldedEventJSON(unfoldedEv, viewStart, viewEnd, h.timezoneOnsets)
			h.addSeriesInstances(payload, unfoldedEv, viewStart, viewEnd)
			payload["calendarId"] = cal.ID
			payload["calendarName"] = cal.Name
			payload["calendarColor"] = color

			canEdit := decide(resourcePath, "write-content")
			canUnbind := decide(resourcePath, "unbind")
			payload["canEdit"] = canEdit
			payload["canDelete"] = canUnbind
			payload["canMove"] = canEdit && canUnbind
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

// importAccess is what an import into a calendar may write: everything the
// collection grants bind or write-content on, or only the events a
// resource-level grant lets the caller change.
type importAccess int

const (
	importForbidden importAccess = iota
	importPerEvent
	importCollection
)

// calendarImportAccess decides an import's access before any of the upload is
// read: from the collection's own grants, else from whether any resource
// beneath it grants the caller write-content.
func (h *Handler) calendarImportAccess(ctx context.Context, user *store.User, cal *store.CalendarAccess) (importAccess, error) {
	collectionPath := calendarACLResourcePath(cal.ID)
	for _, privilege := range []string{"bind", "write-content"} {
		allowed, err := h.hasCalendarPrivilege(ctx, user, cal, collectionPath, privilege)
		if err != nil {
			return importForbidden, err
		}
		if allowed {
			return importCollection, nil
		}
	}
	if h.store.ACLEntries == nil {
		return importForbidden, nil
	}
	for _, principal := range calendarApplicablePrincipalHrefs(user) {
		entries, err := h.store.ACLEntries.ListByPrincipal(ctx, principal)
		if err != nil {
			return importForbidden, err
		}
		for _, entry := range entries {
			if !entry.IsGrant || !strings.HasPrefix(entry.ResourcePath, collectionPath+"/") {
				continue
			}
			allowed, err := h.hasCalendarPrivilege(ctx, user, cal, entry.ResourcePath, "write-content")
			if err != nil {
				return importForbidden, err
			}
			if allowed {
				return importPerEvent, nil
			}
		}
	}
	return importForbidden, nil
}

// importTarget is the stored event an import of uid replaces, if any, and
// whether the caller may write it: bind on the resource a new event takes,
// write-content on the one an existing event has.
func (h *Handler) importTarget(ctx context.Context, user *store.User, cal *store.CalendarAccess, uid string) (*store.Event, bool, error) {
	existing, err := h.store.Events.GetByUID(ctx, cal.ID, uid)
	if err != nil {
		return nil, false, err
	}
	privilege := "bind"
	if existing != nil {
		privilege = "write-content"
	}
	allowed, err := h.hasCalendarPrivilege(ctx, user, cal, calendarEventResourcePath(cal.ID, calendarEventResourceName(uid, existing)), privilege)
	return existing, allowed, err
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
	// A caller who can write nothing in the calendar is refused before the
	// upload is read, so none of it is validated.
	access, err := h.calendarImportAccess(r.Context(), user, cal)
	if err != nil {
		http.Error(w, "failed to check calendar access", http.StatusInternalServerError)
		return
	}
	if access == importForbidden {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	if !calendarAcceptsUIWrite(cal) {
		h.redirect(w, r, fmt.Sprintf("/calendars/%d", calendarID), map[string]string{"error": "this calendar does not accept events"})
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

	events, leftOut, err := utils.ParseICSFile(string(icsData))
	if err != nil {
		h.redirect(w, r, fmt.Sprintf("/calendars/%d", calendarID), map[string]string{"error": fmt.Sprintf("invalid ICS file: %v", err)})
		return
	}

	// Items the file holds that are not events, or that the validator refuses,
	// are skipped so the rest of the file still imports.
	//
	// Where the collection grants the import its writes, an event is validated
	// before its lookup and privilege check, which only what will be stored
	// needs, and a forbidden event fails the whole import before anything is
	// stored. Where only resource-level grants do, each event's privilege is
	// decided first, so an event the caller cannot write costs no validation,
	// and is skipped.
	refused := leftOut
	pending := make([]store.Event, 0, len(events))
	for _, eventICAL := range events {
		uid := utils.ExtractUID(eventICAL)
		if uid == "" {
			uid = utils.GenerateUID()
			eventICAL = utils.EnsureUID(eventICAL, uid)
		}
		var existing *store.Event
		decided := false
		if access == importPerEvent {
			allowed := false
			existing, allowed, err = h.importTarget(r.Context(), user, cal, uid)
			if err != nil {
				http.Error(w, "failed to load event", http.StatusInternalServerError)
				return
			}
			if !allowed {
				refused++
				continue
			}
			decided = true
		}
		event, err := h.validatedEvent(cal, "", eventICAL)
		if err != nil || event.UID != uid {
			refused++
			continue
		}
		if !decided {
			allowed := false
			existing, allowed, err = h.importTarget(r.Context(), user, cal, uid)
			if err != nil {
				http.Error(w, "failed to load event", http.StatusInternalServerError)
				return
			}
			if !allowed {
				http.Error(w, "forbidden", http.StatusForbidden)
				return
			}
		}
		event.ResourceName = calendarEventResourceName(uid, existing)
		pending = append(pending, event)
	}

	imported := 0
	failed := 0
	for _, event := range pending {
		if _, err := h.store.Events.Upsert(r.Context(), event); err != nil {
			failed++
			continue
		}
		imported++
	}

	var skippedParts []string
	if refused > 0 {
		skippedParts = append(skippedParts, fmt.Sprintf("skipped %d (not valid or not events)", refused))
	}
	if failed > 0 {
		skippedParts = append(skippedParts, fmt.Sprintf("%d could not be saved", failed))
	}
	if imported == 0 {
		h.redirect(w, r, fmt.Sprintf("/calendars/%d", calendarID), map[string]string{
			"error": strings.Join(append([]string{"failed to import events"}, skippedParts...), "; "),
		})
		return
	}
	h.redirect(w, r, fmt.Sprintf("/calendars/%d", calendarID), map[string]string{
		"status": strings.Join(append([]string{fmt.Sprintf("Imported %d event(s)", imported)}, skippedParts...), "; "),
	})
}

func eventReturnPath(r *http.Request, calendarID int64) string {
	fallback := fmt.Sprintf("/calendars/%d", calendarID)
	raw := strings.TrimSpace(r.FormValue("return_to"))
	if raw == "" {
		return fallback
	}
	target, err := url.Parse(raw)
	if err != nil || target.IsAbs() || target.Host != "" {
		return fallback
	}
	// Only the aggregated view or this calendar's own page may be returned to, and only
	// their view/date state is preserved.
	if target.Path != "/calendars/all" && target.Path != fallback {
		return fallback
	}

	query := url.Values{}
	switch target.Query().Get("view") {
	case "day", "week", "month", "list":
		query.Set("view", target.Query().Get("view"))
	}
	if date := target.Query().Get("date"); date != "" {
		if _, err := time.Parse("2006-01-02", date); err == nil {
			query.Set("date", date)
		}
	}
	if encoded := query.Encode(); encoded != "" {
		return target.Path + "?" + encoded
	}
	return target.Path
}

func (h *Handler) redirectToEventPage(w http.ResponseWriter, r *http.Request, calendarID int64, params map[string]string) {
	target, err := url.Parse(eventReturnPath(r, calendarID))
	if err != nil {
		h.redirect(w, r, fmt.Sprintf("/calendars/%d", calendarID), params)
		return
	}
	query := target.Query()
	for key, value := range params {
		if value != "" {
			query.Set(key, value)
		}
	}
	target.RawQuery = query.Encode()
	http.Redirect(w, r, target.String(), http.StatusFound)
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
		h.redirectToEventPage(w, r, calendarID, map[string]string{"error": "summary is required"})
		return
	}

	dtstart := strings.TrimSpace(r.FormValue("dtstart"))
	dtend := strings.TrimSpace(r.FormValue("dtend"))
	if dtstart == "" || dtend == "" {
		h.redirectToEventPage(w, r, calendarID, map[string]string{"error": "start and end are required"})
		return
	}

	allDay := r.FormValue("all_day") == "on"
	opts := parseEventOptions(r)

	// Validate date format and range
	if err := utils.ValidateEventDates(dtstart, dtend, allDay, utils.EventZoneIn(opts.Timezone, nil)); err != nil {
		h.redirectToEventPage(w, r, calendarID, map[string]string{"error": err.Error()})
		return
	}

	location := strings.TrimSpace(r.FormValue("location"))
	description := strings.TrimSpace(r.FormValue("description"))

	recurrence, err := utils.ParseRecurrenceOptions(r)
	if err != nil {
		h.redirectToEventPage(w, r, calendarID, map[string]string{"error": err.Error()})
		return
	}

	uid := utils.GenerateUID()
	if err := h.requireCalendarPrivilege(r.Context(), user, cal, calendarEventResourcePath(calendarID, uid), "bind"); err != nil {
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}
	if !calendarAcceptsUIWrite(cal) {
		h.redirectToEventPage(w, r, calendarID, map[string]string{"error": "this calendar does not accept events"})
		return
	}
	body, err := utils.BuildEvent(uid, summary, dtstart, dtend, allDay, location, description, recurrence, opts)
	if err != nil {
		h.redirectToEventPage(w, r, calendarID, map[string]string{"error": err.Error()})
		return
	}
	event, err := h.validatedEvent(cal, uid, body)
	if err != nil {
		h.redirectToEventPage(w, r, calendarID, map[string]string{"error": invalidEventMessage(eventNotSaved, err)})
		return
	}

	if _, err := h.store.Events.Upsert(r.Context(), event); err != nil {
		h.redirectToEventPage(w, r, calendarID, map[string]string{"error": "failed to create event"})
		return
	}

	h.redirectToEventPage(w, r, calendarID, map[string]string{"status": "event_created"})
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

	uid := resourceUIDParam(r)
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

	destinationCalendarID := calendarID
	if rawDestinationID := strings.TrimSpace(r.FormValue("destination_calendar_id")); rawDestinationID != "" {
		destinationCalendarID, err = strconv.ParseInt(rawDestinationID, 10, 64)
		if err != nil || destinationCalendarID <= 0 {
			h.redirectToEventPage(w, r, calendarID, map[string]string{"error": "invalid destination calendar"})
			return
		}
	}
	movingCalendar := destinationCalendarID != calendarID
	if movingCalendar && editScope == "occurrence" {
		h.redirectToEventPage(w, r, calendarID, map[string]string{"error": "move the recurring series before editing one occurrence"})
		return
	}

	resourceName := uid
	if existing.ResourceName != "" {
		resourceName = existing.ResourceName
	}
	destinationCalendar := cal
	if movingCalendar {
		sourcePath := calendarEventResourcePath(calendarID, resourceName)
		if err := h.requireCalendarPrivilege(r.Context(), user, cal, sourcePath, "unbind"); err != nil {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}

		destinationCalendar, err = h.store.Calendars.GetAccessible(r.Context(), destinationCalendarID, user.ID)
		if err != nil {
			http.Error(w, "failed to load destination calendar", http.StatusInternalServerError)
			return
		}
		if destinationCalendar == nil {
			http.Error(w, "destination calendar not found", http.StatusNotFound)
			return
		}
		destinationPath := calendarEventResourcePath(destinationCalendarID, resourceName)
		if err := h.requireCalendarPrivilege(r.Context(), user, destinationCalendar, destinationPath, "bind"); err != nil {
			http.Error(w, "forbidden", http.StatusForbidden)
			return
		}
		if !calendarAcceptsUIWrite(destinationCalendar) {
			h.redirectToEventPage(w, r, calendarID, map[string]string{"error": "destination calendar does not accept events"})
			return
		}

		existingByUID, err := h.store.Events.GetByUID(r.Context(), destinationCalendarID, uid)
		if err != nil {
			http.Error(w, "failed to check destination calendar", http.StatusInternalServerError)
			return
		}
		existingByName, err := h.store.Events.GetByResourceName(r.Context(), destinationCalendarID, resourceName)
		if err != nil {
			http.Error(w, "failed to check destination calendar", http.StatusInternalServerError)
			return
		}
		if existingByUID != nil || existingByName != nil {
			h.redirectToEventPage(w, r, calendarID, map[string]string{"error": "destination calendar already contains this event"})
			return
		}
	}

	summary := strings.TrimSpace(r.FormValue("summary"))
	if summary == "" {
		h.redirectToEventPage(w, r, calendarID, map[string]string{"error": "summary is required"})
		return
	}

	dtstart := strings.TrimSpace(r.FormValue("dtstart"))
	dtend := strings.TrimSpace(r.FormValue("dtend"))
	if dtstart == "" || dtend == "" {
		h.redirectToEventPage(w, r, calendarID, map[string]string{"error": "start and end are required"})
		return
	}

	allDay := r.FormValue("all_day") == "on"
	opts := parseEventOptions(r)
	opts.Object = withoutServerZoneDefinitions(utils.UnfoldLines(existing.RawICAL))

	// Validate date format and range
	if err := utils.ValidateEventDates(dtstart, dtend, allDay, utils.EventZoneIn(opts.Timezone, opts.Object)); err != nil {
		h.redirectToEventPage(w, r, calendarID, map[string]string{"error": err.Error()})
		return
	}

	location := strings.TrimSpace(r.FormValue("location"))
	description := strings.TrimSpace(r.FormValue("description"))

	recurrence, err := utils.ParseRecurrenceOptions(r)
	if err != nil {
		h.redirectToEventPage(w, r, calendarID, map[string]string{"error": err.Error()})
		return
	}

	header, components, footer := utils.SplitComponentLines(opts.Object)
	ical := ""
	if editScope == "occurrence" && recurrenceID != "" && existing.RawICAL != "" {
		// Update only a single occurrence by replacing/adding a RECURRENCE-ID component.
		override, err := utils.BuildEventComponent(uid, summary, dtstart, dtend, allDay, location, description, nil, "", opts)
		if err != nil {
			h.redirectToEventPage(w, r, calendarID, map[string]string{"error": err.Error()})
			return
		}
		recurrenceZone := strings.TrimSpace(r.FormValue("recurrence_timezone"))
		if recurrenceZone == "" {
			recurrenceZone = opts.Timezone
		}
		recLine, err := utils.FormatICalDateTimeIn(recurrenceID, recurrenceAllDay, false, "RECURRENCE-ID", utils.EventZoneIn(recurrenceZone, opts.Object))
		if err != nil {
			h.redirectToEventPage(w, r, calendarID, map[string]string{"error": "invalid occurrence date"})
			return
		}
		if recLine != "" {
			if len(override) >= 2 {
				override = append(override[:2], append([]string{recLine}, override[2:]...)...)
			} else {
				override = append([]string{recLine}, override...)
			}
		}

		var newComponents [][]string
		var previous []string
		for _, comp := range components {
			if utils.SameRecurrenceID(comp, override) {
				previous = comp
				continue
			}
			if previous == nil && utils.RecurrenceIDValue(comp) == "" {
				previous = comp
			}
			newComponents = append(newComponents, comp)
		}
		if previous != nil {
			override = withUneditedOverrideProperties(override, previous, opts)
		}
		newComponents = append(newComponents, override)
		ical = utils.BuildFromComponents(header, newComponents, footer)
	} else {
		// Update the series/master event while keeping overrides intact.
		master, err := utils.BuildEventComponent(uid, summary, dtstart, dtend, allDay, location, description, recurrence, "", opts)
		if err != nil {
			h.redirectToEventPage(w, r, calendarID, map[string]string{"error": err.Error()})
			return
		}
		replaced := false
		for i, comp := range components {
			if utils.RecurrenceIDValue(comp) == "" && !replaced {
				components[i] = withUneditedProperties(master, comp, opts.Object, opts, recurrence)
				components = overridesFollowingSeries(components, comp, components[i], opts.Object)
				replaced = true
				break
			}
		}
		if !replaced {
			components = append([][]string{master}, components...)
		}
		ical = utils.BuildFromComponents(header, components, footer)
	}
	updatedEvent, err := h.validatedEvent(destinationCalendar, resourceName, ical)
	if err != nil {
		h.redirectToEventPage(w, r, calendarID, map[string]string{"error": invalidEventMessage(eventNotSaved, err)})
		return
	}
	if movingCalendar {
		fromPath := calendarEventResourcePath(calendarID, resourceName)
		toPath := calendarEventResourcePath(destinationCalendarID, resourceName)
		err = h.store.UpdateAndMoveEventAndState(r.Context(), calendarID, updatedEvent, fromPath, toPath)
	} else {
		_, err = h.store.Events.Upsert(r.Context(), updatedEvent)
	}
	if err != nil {
		message := "failed to update event"
		if err == store.ErrConflict {
			message = "destination calendar already contains this event"
		}
		h.redirectToEventPage(w, r, calendarID, map[string]string{"error": message})
		return
	}

	h.redirectToEventPage(w, r, calendarID, map[string]string{"status": "event_updated"})
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

	uid := resourceUIDParam(r)
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

		timezone := strings.TrimSpace(r.FormValue("recurrence_timezone"))
		if timezone == "" {
			timezone = strings.TrimSpace(r.FormValue("timezone"))
		}
		// The occurrence is removed as an edit of it is stored: from the object
		// less the definitions the server wrote, read in the host's zones,
		// whose definitions the validator stores afresh.
		object := withoutServerZoneDefinitions(utils.UnfoldLines(existing.RawICAL))
		exdateLine, err := utils.FormatICalDateTimeIn(recurrenceID, recurrenceAllDay, false, "EXDATE", utils.EventZoneIn(timezone, object))
		if err != nil || exdateLine == "" {
			h.redirectToEventPage(w, r, calendarID, map[string]string{"error": "invalid recurrence id"})
			return
		}
		targetValueParts := strings.SplitN(exdateLine, ":", 2)
		if len(targetValueParts) != 2 {
			h.redirectToEventPage(w, r, calendarID, map[string]string{"error": "invalid recurrence id"})
			return
		}
		targetValue := targetValueParts[1]

		header, components, footer := utils.SplitComponentLines(object)
		var newComponents [][]string
		masterHandled := false
		targetIdentity := []string{strings.Replace(exdateLine, "EXDATE", "RECURRENCE-ID", 1)}
		for _, comp := range components {
			recID := utils.RecurrenceIDValue(comp)
			if utils.SameRecurrenceID(comp, targetIdentity) {
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
			resourcePath := calendarEventResourcePath(calendarID, calendarEventResourceName(uid, existing))
			if err := h.store.DeleteEventAndState(r.Context(), calendarID, store.EventDAVResourceState(existing), resourcePath, nil); err != nil {
				h.redirectToEventPage(w, r, calendarID, map[string]string{"error": "failed to delete event"})
				return
			}
			h.redirectToEventPage(w, r, calendarID, map[string]string{"status": "event_deleted"})
			return
		}

		resourceName := uid
		if existing.ResourceName != "" {
			resourceName = existing.ResourceName
		}
		updatedEvent, err := h.occurrenceRemovedEvent(cal, existing, resourceName, utils.BuildFromComponents(header, newComponents, footer))
		if err != nil {
			h.redirectToEventPage(w, r, calendarID, map[string]string{"error": invalidEventMessage("the occurrence could not be deleted", err)})
			return
		}
		if _, err := h.store.Events.Upsert(r.Context(), updatedEvent); err != nil {
			h.redirectToEventPage(w, r, calendarID, map[string]string{"error": "failed to delete occurrence"})
			return
		}
		h.redirectToEventPage(w, r, calendarID, map[string]string{"status": "occurrence_deleted"})
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
		resourcePath := calendarEventResourcePath(calendarID, calendarEventResourceName(uid, existing))
		if err := h.store.DeleteEventAndState(r.Context(), calendarID, store.EventDAVResourceState(existing), resourcePath, nil); err != nil {
			h.redirectToEventPage(w, r, calendarID, map[string]string{"error": "failed to delete event"})
			return
		}
		h.redirectToEventPage(w, r, calendarID, map[string]string{"status": "event_deleted"})
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
	attendees := splitAddressList(r.FormValue("attendees"))
	attachments := splitAttachmentList(r.FormValue("attachments"))
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

// splitAddressList splits the attendee field into its addresses, each
// "Name <address>" or a bare address: one per line, or several on a line
// separated by commas. A display name may hold a comma itself -- Exchange
// writes "Last, First" -- so a comma separates two addresses only once the
// text before it holds an address, and never inside quotes or angle brackets.
func splitAddressList(value string) []string {
	var out []string
	var current strings.Builder
	quoted, bracketed, addressed := false, false, false
	flush := func() {
		if item := strings.Trim(current.String(), ", \t"); item != "" {
			out = append(out, item)
		}
		current.Reset()
		quoted, bracketed, addressed = false, false, false
	}
	for _, r := range value {
		switch {
		case r == '\n' || r == '\r':
			flush()
			continue
		case r == '"' && !bracketed:
			quoted = !quoted
		case quoted:
		case r == '<':
			bracketed = true
		case r == '>':
			bracketed, addressed = false, true
		case r == '@':
			addressed = true
		case r == ',' && !bracketed && addressed:
			flush()
			continue
		}
		current.WriteRune(r)
	}
	flush()
	return out
}

// splitAttachmentList splits the attachment field into its URIs: one per
// line, or several on a line separated by a comma and whitespace. A URI may
// hold a comma but no whitespace, so a comma alone does not separate two.
func splitAttachmentList(value string) []string {
	var out []string
	add := func(uri string) {
		if uri = strings.Trim(uri, ", \t"); uri != "" {
			out = append(out, uri)
		}
	}
	for _, line := range strings.FieldsFunc(value, func(r rune) bool { return r == '\n' || r == '\r' }) {
		start := 0
		for i := 0; i+1 < len(line); i++ {
			if line[i] == ',' && (line[i+1] == ' ' || line[i+1] == '\t') {
				add(line[start:i])
				start = i + 1
			}
		}
		add(line[start:])
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

// unfoldedEvent is a stored event with the unfolded content lines of its
// object, unfolded once for every reader of the object a request makes.
type unfoldedEvent struct {
	event store.Event
	lines []string
	// series is what lines say about the event's recurrence.
	series eventSeries
}

func unfoldedStoreEvents(events []unfoldedEvent) []store.Event {
	stored := make([]store.Event, len(events))
	for i, ev := range events {
		stored[i] = ev.event
	}
	return stored
}

// filterEventsForMonth returns the events relevant to the given month range,
// each with its unfolded lines. monthStart and monthEnd are UTC midnights that
// stand for the dates a viewer asked for, wherever the viewer is. Each object
// is unfolded as it is read, and its lines kept only when the event is
// relevant, so a request holds the lines of the events it shows rather than of
// the whole calendar.
func filterEventsForMonth(allEvents []store.Event, monthStart, monthEnd time.Time) []unfoldedEvent {
	var relevant []unfoldedEvent
	for _, ev := range allEvents {
		unfolded := unfoldedEvent{event: ev, lines: utils.UnfoldLines(ev.RawICAL)}
		unfolded.series = readEventSeries(unfolded.lines)
		series := unfolded.series
		rangeStart, rangeEnd := monthFilterRange(unfolded.lines)
		start := eventMonthFilterStart(ev, rangeStart)

		if series.recurs() {
			if start != nil && !start.earliestViewedDate().After(monthEnd) && !series.endsBefore(monthStart, unfolded.lines) {
				relevant = append(relevant, unfolded)
			}
		} else {
			end := eventMonthFilterEnd(ev, start, rangeEnd)
			if start != nil && end != nil && !start.earliestViewedDate().After(monthEnd) && !end.latestViewedDate().Before(monthStart) {
				relevant = append(relevant, unfolded)
			}
		}
	}
	return relevant
}

// filterDate is a date the month filter places an event on: the date part of
// a DTSTART or DTEND as written, held as that date's UTC midnight. reach is
// how many days from it the date a viewer sees the time on can lie. A DATE is
// on the date every viewer sees. A UTC time is an instant a viewer sees within
// a day of its date, and so is a floating time, which the views read in UTC
// as the server does. A time in a TZID's zone is one whose date there is
// itself within a day of its UTC date.
type filterDate struct {
	date  time.Time
	reach int
}

// earliestViewedDate is the earliest date a viewer anywhere sees d on.
func (d filterDate) earliestViewedDate() time.Time {
	return d.date.AddDate(0, 0, -d.reach)
}

// latestViewedDate is the latest date a viewer anywhere sees d on.
func (d filterDate) latestViewedDate() time.Time {
	return d.date.AddDate(0, 0, d.reach)
}

// instantReach is the reach of an instant held as its UTC date.
const instantReach = 1

// maxUTCOffset is the furthest east of UTC any zone's clocks run.
const maxUTCOffset = 14 * time.Hour

// rruleUntilEndsBefore reports whether a series whose RRULE ends at the UNTIL
// value until has no occurrence on or after day, a UTC midnight that stands for
// that date wherever the calendar is viewed. dtstart is the series' DTSTART
// content line and object the unfolded object holding it. A UTC UNTIL is an
// instant whose date a viewer east of UTC sees up to maxUTCOffset later. A
// DATE UNTIL is inclusive of its day (RFC 5545 §3.3.10), and is read in the
// DTSTART's zone, as the DAV layer reads it, so the series may have an
// instance until that day ends there, which a viewer east of UTC sees up to
// maxUTCOffset later still. A timed series with neither a TZID nor UTC times
// is read in UTC, which the views show as instants too, and so is its
// floating UNTIL. In a series of dates, the date is the one every viewer
// sees. An UNTIL it cannot read, or a series without a DTSTART, ends nothing.
func rruleUntilEndsBefore(until string, day time.Time, dtstart string, object []string) bool {
	if instant, err := time.Parse("20060102T150405Z", until); err == nil {
		return instant.Add(maxUTCOffset).Before(day)
	}
	if len(until) < 8 {
		return false
	}
	date, err := time.Parse("20060102", until[:8])
	if err != nil {
		return false
	}
	keyPart, value, ok := ical.SplitContentLine(dtstart)
	if !ok {
		return false
	}
	value = strings.TrimSpace(value)
	tzid, zoned := ical.PropertyParam(keyPart, "TZID")
	utc := strings.HasSuffix(strings.ToUpper(value), "Z")
	if ical.PropertyParamEquals(keyPart, "VALUE", "DATE") || len(value) == len("20060102") {
		return date.Before(day)
	}
	if !utc && !zoned {
		if instant, err := time.Parse("20060102T150405", until); err == nil {
			return instant.Add(maxUTCOffset).Before(day)
		}
	}
	end := date.AddDate(0, 0, 1)
	if zoned && !utc {
		end = utils.EventZoneIn(tzid, object).Instant(end)
	}
	return !end.Add(maxUTCOffset).After(day)
}

// eventComponentTimes are a VEVENT's first DTSTART, DTEND and DURATION lines,
// each "" when it has none.
type eventComponentTimes struct {
	dtstart, dtend, duration string
}

// eventSeriesRule is an RRULE of a VEVENT among an object's content lines,
// with that VEVENT's times.
type eventSeriesRule struct {
	eventComponentTimes
	rrule string
}

// eventSeries is what an object's VEVENTs say about its recurrence: their
// RRULEs, whether any adds instances by RDATE, and the last date anything
// other than a rule places an instance on: an RDATE, an override, and a
// series' DTSTART when no rule follows it.
type eventSeries struct {
	rules  []eventSeriesRule
	rdates bool
	// explicitEnds holds, for each instance an RDATE, an override or a
	// rule-less DTSTART places, the date it ends on.
	explicitEnds []filterDate
}

// readEventSeries reads the series of the VEVENTs among an object's unfolded
// content lines, a VEVENT left open included. A VTIMEZONE's rules are not the
// object's recurrence.
func readEventSeries(unfolded []string) eventSeries {
	type component struct {
		times    eventComponentTimes
		rrules   []string
		rdates   []string
		override bool
	}
	var components []component
	var current *component
	for _, line := range unfolded {
		switch {
		case strings.EqualFold(line, "BEGIN:VEVENT"):
			components = append(components, component{})
			current = &components[len(components)-1]
			continue
		case strings.EqualFold(line, "END:VEVENT"):
			current = nil
			continue
		case current == nil:
			continue
		}
		key, _, value, ok := parseICalProperty(line)
		switch {
		case !ok:
		case key == "RRULE":
			current.rrules = append(current.rrules, value)
		case key == "RDATE":
			current.rdates = append(current.rdates, line)
		case key == "RECURRENCE-ID":
			current.override = true
		case key == "DTSTART" && current.times.dtstart == "":
			current.times.dtstart = line
		case key == "DTEND" && current.times.dtend == "":
			current.times.dtend = line
		case key == "DURATION" && current.times.duration == "":
			current.times.duration = line
		}
	}

	var series eventSeries
	var seriesLength time.Duration
	for _, c := range components {
		if c.override {
			continue
		}
		seriesLength = max(seriesLength, c.times.length())
		for _, rrule := range c.rrules {
			series.rules = append(series.rules, eventSeriesRule{eventComponentTimes: c.times, rrule: rrule})
		}
	}
	for _, c := range components {
		length := c.times.length()
		if c.override && length == 0 {
			length = seriesLength
		}
		if len(c.rdates) > 0 {
			series.rdates = true
		}
		for _, line := range c.rdates {
			_, params, value, _ := parseICalProperty(line)
			for _, date := range strings.Split(value, ",") {
				start, period, isPeriod := strings.Cut(date, "/")
				periodLength := length
				if isPeriod {
					// A period (RFC 5545 §3.3.9) states its own end or length.
					if duration, ok := ical.ParseDuration(strings.TrimSpace(period)); ok {
						periodLength = max(duration, 0)
					} else if end := rawICalCalendarDate(period, params, false); end != nil {
						series.addExplicitEnd(end, 0)
					}
				}
				series.addExplicitEnd(rawICalCalendarDate(start, params, false), periodLength)
			}
		}
		if c.override || len(c.rrules) == 0 && len(c.rdates) > 0 {
			if _, params, value, ok := parseICalProperty(c.times.dtstart); ok {
				series.addExplicitEnd(rawICalCalendarDate(value, params, false), length)
			}
		}
	}
	return series
}

// addExplicitEnd records an instance starting on start, lasting length.
// A start it cannot read places an instance it cannot date, which no view
// may leave out, so it is recorded as ending at the latest date there is.
func (s *eventSeries) addExplicitEnd(start *filterDate, length time.Duration) {
	if start == nil {
		_, latest := ical.DateLimits()
		s.explicitEnds = append(s.explicitEnds, filterDate{date: latest})
		return
	}
	end := *start
	end.date = end.date.Add(length)
	s.explicitEnds = append(s.explicitEnds, end)
}

// recurs reports whether the object's VEVENTs repeat by rule or by date.
func (s eventSeries) recurs() bool {
	return len(s.rules) > 0 || s.rdates
}

// endsBefore reports whether the series has no instance a viewer sees on
// or after day, a UTC midnight standing for that date wherever the calendar
// is viewed: no RDATE, override or rule-less DTSTART places one there, and,
// for a series repeating by rule, some rule's UNTIL ends it before day,
// counting the days an instance lasts. object is the unfolded object.
func (s eventSeries) endsBefore(day time.Time, object []string) bool {
	for _, end := range s.explicitEnds {
		if !end.latestViewedDate().Before(day) {
			return false
		}
	}
	if len(s.rules) == 0 {
		return len(s.explicitEnds) > 0
	}
	for _, rule := range s.rules {
		if rruleUntilEndsBefore(rruleUntil(rule.rrule), day.Add(-rule.length()), rule.dtstart, object) {
			return true
		}
	}
	return false
}

// length is how much later than its start an instance of the VEVENT can end
// on a viewer's calendar: its DURATION, or the days from its DTSTART's date
// to its DTEND's.
func (t eventComponentTimes) length() time.Duration {
	if _, value, ok := ical.SplitContentLine(t.duration); ok {
		if length, ok := ical.ParseDuration(strings.TrimSpace(value)); ok && length > 0 {
			return length
		}
	}
	_, startParams, startValue, okStart := parseICalProperty(t.dtstart)
	_, endParams, endValue, okEnd := parseICalProperty(t.dtend)
	if !okStart || !okEnd {
		return 0
	}
	start, end := rawICalCalendarDate(startValue, startParams, false), rawICalCalendarDate(endValue, endParams, true)
	if start == nil || end == nil || !end.date.After(start.date) {
		return 0
	}
	return end.date.Sub(start.date)
}

// eventMonthFilterStart is the date the month filter starts ev on: start,
// what monthFilterRange read from its object, else its stored dtstart.
func eventMonthFilterStart(ev store.Event, start *filterDate) *filterDate {
	if start != nil {
		return start
	}
	if ev.DTStart == nil {
		return nil
	}
	return &filterDate{date: time.Date(ev.DTStart.Year(), ev.DTStart.Month(), ev.DTStart.Day(), 0, 0, 0, 0, time.UTC), reach: storedDateReach(ev)}
}

// eventMonthFilterEnd is the date the month filter ends ev on: end, what
// monthFilterRange read from its object, else its stored dtend, and never
// before start.
func eventMonthFilterEnd(ev store.Event, start, end *filterDate) *filterDate {
	if end == nil && ev.DTEnd != nil {
		end = &filterDate{date: time.Date(ev.DTEnd.Year(), ev.DTEnd.Month(), ev.DTEnd.Day(), 0, 0, 0, 0, time.UTC), reach: storedDateReach(ev)}
	}
	if end == nil {
		return start
	}
	if start != nil && end.date.Before(start.date) {
		return start
	}
	return end
}

// storedDateReach is the reach of ev's stored dtstart and dtend columns: the
// instants a timed event's times name, and the dates of an all-day one.
func storedDateReach(ev store.Event) int {
	if ev.AllDay {
		return 0
	}
	return instantReach
}

// monthFilterRange reads the dates the month filter places an event on from
// its object's unfolded content lines: the master VEVENT's DTSTART and DTEND,
// else the first of an override's.
func monthFilterRange(lines []string) (*filterDate, *filterDate) {
	inEvent := false
	componentHasRecurrenceID := false
	var startFallback, endFallback *filterDate

	for _, line := range lines {
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

func rawICalCalendarDate(value string, params map[string]string, isEnd bool) *filterDate {
	value = strings.TrimSpace(value)
	if len(value) < 8 {
		return nil
	}
	parsed, err := time.Parse("20060102", value[:8])
	if err != nil {
		return nil
	}
	date := filterDate{date: parsed}
	if strings.EqualFold(params["VALUE"], "DATE") || len(value) == 8 {
		if isEnd {
			date.date = date.date.AddDate(0, 0, -1)
		}
		return &date
	}
	if strings.TrimSpace(params["TZID"]) != "" && !strings.HasSuffix(strings.ToUpper(value), "Z") {
		date.reach = instantReach + 1
	} else {
		date.reach = instantReach
	}
	return &date
}

// unfoldedEventJSON is the view model of an event shown from viewStart to
// viewEnd, the instants the browser's view can show (eventViewWindow), reading the rules of the zones the
// object defines through onsets, or through listings of its own when onsets is
// nil.
func unfoldedEventJSON(unfolded unfoldedEvent, viewStart, viewEnd time.Time, onsets ical.ObservanceOnsets) map[string]any {
	ev, lines := unfolded.event, unfolded.lines
	payload := map[string]any{
		"uid":     ev.UID,
		"ical":    ev.RawICAL,
		"lastMod": ev.LastModified.Format(time.RFC3339),
	}

	zones := calendarEventZones{times: ical.NewObjectTimesSharingOnsets(lines, onsets)}
	parsed := parseCalendarEventMetadata(ev, lines, &zones)
	parsed.addToPayload(payload)
	if parsed.RRule != "" {
		if form, ok := seriesRecurrenceForm(withoutServerZoneDefinitions(lines)); ok {
			payload["recurrenceForm"] = form
		}
	}
	if offsets := calendarEventTimezoneOffsets(parsed, viewStart, viewEnd, &zones); len(offsets) > 0 {
		payload["timezoneOffsets"] = offsets
	}
	return payload
}

// recurrenceForm is the view model of the editor's repeat fields for a
// series, which both editors fill them from, as shownRecurrence shows its
// rule: an edit that submits them as they are keeps the rule as written.
type recurrenceForm struct {
	Frequency string   `json:"frequency"`
	Interval  int      `json:"interval"`
	ByDay     []string `json:"byDay"`
	// ByMonthDay is the days of the month as the field holds them.
	ByMonthDay string `json:"byMonthDay"`
	// ByMonth is the month, or 0 for any.
	ByMonth int `json:"byMonth"`
	// End is the end field's value: "never", "after" Count instances, or
	// "on" Until, a YYYY-MM-DD date.
	End   string `json:"end"`
	Count int    `json:"count"`
	Until string `json:"until"`
	// Partial is true when the rule holds more than the fields show.
	Partial bool `json:"partial"`
}

// seriesRecurrenceForm is the view model of the repeat fields for the series
// an object holds, its unfolded content lines less the zone definitions an
// edit drops (withoutServerZoneDefinitions): the rule of its master, the
// VEVENT a series edit replaces. ok is false when that master does not repeat
// by a rule.
func seriesRecurrenceForm(object []string) (recurrenceForm, bool) {
	_, components, _ := utils.SplitComponentLines(object)
	for _, component := range components {
		if utils.RecurrenceIDValue(component) != "" {
			continue
		}
		properties, _ := splitComponentBody(component)
		shown, partial, ok := shownRecurrence(properties, object)
		if !ok {
			return recurrenceForm{}, false
		}
		form := recurrenceForm{
			Frequency: shown.Frequency,
			Interval:  shown.Interval,
			ByDay:     shown.ByDay,
			ByMonth:   shown.ByMonth,
			End:       "never",
			Partial:   partial,
		}
		if form.ByDay == nil {
			form.ByDay = []string{}
		}
		days := make([]string, len(shown.ByMonthDay))
		for i, day := range shown.ByMonthDay {
			days[i] = strconv.Itoa(day)
		}
		form.ByMonthDay = strings.Join(days, ",")
		switch {
		case shown.Count > 0:
			form.End, form.Count = "after", shown.Count
		case shown.Until != "":
			form.End, form.Until = "on", shown.Until
		}
		return form, true
	}
	return recurrenceForm{}, false
}

// calendarTimezoneOffset is one entry of a zone's offset listing in the view
// model: the zone keeps Offset seconds east of UTC from the instant From on.
type calendarTimezoneOffset struct {
	From   string `json:"from"`
	Offset int    `json:"offset"`
}

// calendarEventTimezoneOffsets lists, for each zone an event's times are wall
// clocks in that the object itself defines, the offsets that definition keeps
// at the instants the browser reads in it. The browser need not know such a
// zone -- Outlook's "Eastern Standard Time" -- or may define it differently,
// while the DAV layer reads the object's definition. A zone read through the
// host's database, which the browser shares, is left out.
//
// The browser reads the event's and its overrides' own times and UNTIL, and
// the instances it shows from viewStart to viewEnd; each is listed with two
// days either side, more than any offset. Between them the browser reads the
// entry before, which no time it reads falls under.
func calendarEventTimezoneOffsets(event calendarEventMetadata, viewStart, viewEnd time.Time, zones *calendarEventZones) map[string][]calendarTimezoneOffset {
	tzids := make(map[string]struct{})
	var points []time.Time
	include := func(t *time.Time) {
		if t != nil {
			points = append(points, *t)
		}
	}
	for _, component := range append([]calendarEventMetadata{event}, event.Overrides...) {
		for _, tzid := range []string{component.Timezone, component.RecurrenceTimezone} {
			if tzid != "" && tzid != "UTC" {
				tzids[tzid] = struct{}{}
			}
		}
		include(component.DTStart)
		include(component.DTEnd)
		include(component.RecurrenceID)
	}
	if len(tzids) == 0 || len(points) == 0 {
		return nil
	}
	if until, err := ical.ParseDateTime(event.RRuleParsed["UNTIL"]); err == nil {
		until = until.Add(24 * time.Hour)
		include(&until)
	}
	const reach = 48 * time.Hour
	type span struct{ from, through time.Time }
	var spans []span
	for _, point := range points {
		spans = append(spans, span{point.Add(-reach), point.Add(reach)})
	}
	if !viewEnd.IsZero() {
		spans = append(spans, span{viewStart.Add(-reach), viewEnd.Add(reach)})
	}
	slices.SortFunc(spans, func(a, b span) int { return a.from.Compare(b.from) })
	merged := spans[:1]
	for _, s := range spans[1:] {
		if last := &merged[len(merged)-1]; !s.from.After(last.through) {
			last.through = maxTime(last.through, s.through)
			continue
		}
		merged = append(merged, s)
	}

	listings := make(map[string][]calendarTimezoneOffset)
	for tzid := range tzids {
		zone := zones.times.Timezone(tzid)
		var listing []calendarTimezoneOffset
		listed := true
		for _, s := range merged {
			offsets, ok := zone.Offsets(s.from, s.through)
			if !ok {
				listed = false
				break
			}
			for _, offset := range offsets {
				seconds := int(offset.Offset / time.Second)
				if len(listing) > 0 && listing[len(listing)-1].Offset == seconds {
					continue
				}
				listing = append(listing, calendarTimezoneOffset{From: offset.From.UTC().Format(time.RFC3339), Offset: seconds})
			}
		}
		if listed {
			listings[tzid] = listing
		}
	}
	return listings
}

func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
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
	RecurrenceTimezone string
	// RecurrenceRange is "THISANDFUTURE" for an override of its instance and
	// every later one (RFC 5545 §3.8.4.4), and "" for any other.
	RecurrenceRange string
	Overrides       []calendarEventMetadata
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
		payload["recurrenceTimezone"] = m.RecurrenceTimezone
		if m.RecurrenceRange != "" {
			payload["recurrenceRange"] = m.RecurrenceRange
		}
	}
	if len(m.Overrides) > 0 {
		overrides := make([]map[string]any, 0, len(m.Overrides))
		for _, override := range m.Overrides {
			overridePayload := make(map[string]any)
			override.addToPayload(overridePayload)
			// The browser shows an override as its series with the
			// override's fields over it, and an override replaces its
			// instance (RFC 5545 §3.8.4.4): a field it leaves out is
			// empty, not the series'. Only the summary, which the editor
			// requires, is the series' when the override has none.
			for key, empty := range emptyOverrideFields {
				if _, set := overridePayload[key]; !set {
					overridePayload[key] = empty
				}
			}
			overrides = append(overrides, overridePayload)
		}
		payload["overrides"] = overrides
	}
}

// emptyOverrideFields are the view model's values for the descriptive fields
// an override leaves out.
var emptyOverrideFields = map[string]any{
	"description": "", "htmlDescription": "", "location": "", "status": "", "url": "", "class": "",
	"transp": "", "organizer": "", "categories": []string{}, "attendees": []string{},
	"attachments": []string{}, "reminders": []int{},
}

// parseCalendarEventMetadata reads the event whose unfolded content lines are
// lines, its wall clocks in the zones zones resolves.
func parseCalendarEventMetadata(ev store.Event, lines []string, zones *calendarEventZones) calendarEventMetadata {
	components := parseCalendarEventComponents(lines, zones)
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
	if ev.DTStart != nil && master.DTStart == nil {
		master.DTStart = ev.DTStart
	}
	if ev.DTEnd != nil && master.DTEnd == nil {
		master.DTEnd = ev.DTEnd
	}
	master.AllDay = ev.AllDay
	master.Overrides = overrides
	return *master
}

func parseCalendarEventComponents(lines []string, zones *calendarEventZones) []calendarEventMetadata {
	var components []calendarEventMetadata
	var current []string
	inEvent := false

	for _, line := range lines {
		switch {
		case strings.EqualFold(line, "BEGIN:VEVENT"):
			inEvent = true
			current = nil
		case strings.EqualFold(line, "END:VEVENT"):
			if inEvent {
				components = append(components, parseCalendarEventComponent(current, zones))
			}
			inEvent = false
			current = nil
		case inEvent:
			current = append(current, line)
		}
	}

	return components
}

func parseCalendarEventComponent(lines []string, zones *calendarEventZones) calendarEventMetadata {
	var event calendarEventMetadata
	var alarm []string

	for _, line := range lines {
		key, params, value, ok := parseICalProperty(line)
		if !ok {
			continue
		}
		if alarm != nil {
			alarm = append(alarm, line)
			if key == "END" && strings.EqualFold(strings.TrimSpace(value), "VALARM") {
				if minutes, ok := editorReminder(alarm); ok {
					event.Reminders = append(event.Reminders, minutes)
				}
				alarm = nil
			}
			continue
		}
		switch key {
		case "BEGIN":
			if strings.EqualFold(strings.TrimSpace(value), "VALARM") {
				alarm = []string{line}
			}
			continue
		case "END":
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
			if t, allDay := parseCalendarEventDate(value, params, zones); t != nil {
				event.RecurrenceID = t
				event.RecurrenceIDAllDay = allDay
				event.RecurrenceTimezone = params["TZID"]
				if event.RecurrenceTimezone == "" && !allDay {
					event.RecurrenceTimezone = "UTC"
				}
				if strings.EqualFold(strings.TrimSpace(params["RANGE"]), "THISANDFUTURE") {
					event.RecurrenceRange = "THISANDFUTURE"
				}
			}
		case "X-ALT-DESC":
			if strings.EqualFold(params["FMTTYPE"], "text/html") {
				event.HTMLDescription = value
			}
		case "LOCATION":
			event.Location = unescapeICalText(value)
		case "DTSTART":
			if t, allDay := parseCalendarEventDate(value, params, zones); t != nil {
				event.DTStart = t
				event.AllDay = allDay
			}
			if tzid := params["TZID"]; tzid != "" {
				event.Timezone = tzid
			}
		case "DTEND":
			if t, _ := parseCalendarEventDate(value, params, zones); t != nil {
				event.DTEnd = t
			}
			if event.Timezone == "" {
				event.Timezone = params["TZID"]
			}
		case "DURATION":
			event.Duration = value
		case "RRULE":
			event.RRule = value
			// Rule part values are case-insensitive, and the editors'
			// selects hold them upper-cased.
			event.RRuleParsed = ical.RRuleParts(value)
			for part, partValue := range event.RRuleParsed {
				event.RRuleParsed[part] = strings.ToUpper(partValue)
			}
		case "EXDATE":
			event.Exdates = append(event.Exdates, parseCalendarEventDateList(value, params, zones)...)
		case "STATUS":
			event.Status = strings.ToUpper(strings.TrimSpace(value))
		case "CATEGORIES":
			event.Categories = append(event.Categories, splitICalTextList(value)...)
		case "URL":
			event.URL = value
		case "CLASS":
			event.Class = strings.ToUpper(strings.TrimSpace(value))
		case "TRANSP":
			event.Transparency = strings.ToUpper(strings.TrimSpace(value))
		case "ORGANIZER":
			event.Organizer = formatICalEmailValue(value, params)
		case "ATTENDEE":
			event.Attendees = append(event.Attendees, formatICalEmailValue(value, params))
		case "ATTACH":
			if keyPart, _, _ := ical.SplitContentLine(line); !binaryAttachment(keyPart) {
				event.Attachments = append(event.Attachments, value)
			}
		}
	}

	return event
}

func parseICalProperty(line string) (string, map[string]string, string, bool) {
	keyPart, value, ok := ical.SplitContentLine(line)
	if !ok {
		return "", nil, "", false
	}
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

func parseCalendarEventDateList(value string, params map[string]string, zones *calendarEventZones) []time.Time {
	values := strings.Split(value, ",")
	dates := make([]time.Time, 0, len(values))
	for _, v := range values {
		if t, _ := parseCalendarEventDate(v, params, zones); t != nil {
			dates = append(dates, *t)
		}
	}
	return dates
}

func parseCalendarEventDate(value string, params map[string]string, zones *calendarEventZones) (*time.Time, bool) {
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

	wall, err := ical.ParseDateTimeInLocation(strings.TrimSuffix(value, "Z"), time.UTC)
	if err != nil {
		return nil, false
	}
	instant := zones.instant(params["TZID"], wall)
	return &instant, false
}

// calendarEventZones reads the wall clocks of one calendar object in the zones
// its TZIDs name, as the DAV layer reads them: through the VTIMEZONE the object
// ships, else the host's zone of that name, else in UTC. times resolves each
// TZID once for the view model's times and its offset listings alike.
type calendarEventZones struct {
	times *ical.ObjectTimes
}

func (z *calendarEventZones) instant(tzid string, wall time.Time) time.Time {
	if tzid == "" {
		return wall
	}
	if zone := z.times.Timezone(tzid); zone.Defined() {
		return zone.Instant(wall).UTC()
	}
	return wall
}

// rruleUntil is an RRULE's UNTIL, read as ical.RRuleParam reads it.
func rruleUntil(value string) string {
	until, _ := ical.RRuleParam(value, "UNTIL")
	return until
}

// parseTriggerMinutes reads a TRIGGER duration (RFC 5545 §3.3.6) as how many
// minutes before the start it is, rounded to the nearest minute. It fails for a
// duration after the start: the editor writes reminders before it.
func parseTriggerMinutes(value string) (int, bool) {
	value = strings.TrimSpace(value)
	before := strings.HasPrefix(value, "-")
	if before || strings.HasPrefix(value, "+") {
		value = value[1:]
	}
	if !strings.HasPrefix(value, "P") {
		return 0, false
	}

	seconds := 0
	timePart := false
	digits := 0
	read, units, timeUnits := false, 0, 0
	for _, r := range value[1:] {
		unit := 0
		switch r {
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9':
			if digits > 100_000_000 {
				return 0, false
			}
			digits = digits*10 + int(r-'0')
			read = true
			continue
		case 'T':
			if timePart || read {
				return 0, false
			}
			timePart = true
			continue
		case 'W':
			unit = 7 * 24 * 3600
		case 'D':
			unit = 24 * 3600
		case 'H':
			unit = 3600
		case 'M':
			unit = 60
		case 'S':
			unit = 1
		default:
			return 0, false
		}
		if !read || timePart != (r == 'H' || r == 'M' || r == 'S') {
			return 0, false
		}
		if seconds += digits * unit; seconds > 100_000_000*7*24*3600 {
			return 0, false
		}
		digits, read = 0, false
		units++
		if timePart {
			timeUnits++
		}
	}
	if read || units == 0 || (timePart && timeUnits == 0) || (!before && seconds != 0) {
		return 0, false
	}
	return (seconds + 30) / 60, true
}

func unescapeICalText(value string) string {
	value = strings.ReplaceAll(value, "\\n", "\n")
	value = strings.ReplaceAll(value, "\\N", "\n")
	value = strings.ReplaceAll(value, "\\,", ",")
	value = strings.ReplaceAll(value, "\\;", ";")
	value = strings.ReplaceAll(value, "\\\\", "\\")
	return value
}

// splitICalTextList splits a TEXT list value (RFC 5545 §3.1.1) at the commas
// that separate its values, not at an escaped one inside a value, and
// unescapes each.
func splitICalTextList(value string) []string {
	var out []string
	start := 0
	for i := 0; i <= len(value); i++ {
		switch {
		case i < len(value)-1 && value[i] == '\\':
			i++
		case i == len(value) || value[i] == ',':
			if part := strings.TrimSpace(unescapeICalText(value[start:i])); part != "" {
				out = append(out, part)
			}
			start = i + 1
		}
	}
	return out
}

// formatICalEmailValue is how the editor shows an ORGANIZER or ATTENDEE:
// "Name <address>", or the address alone when there is no name or the name is
// the address, as clients with no name for an attendee write it. Any other
// name holding what would read as an address of its own -- an @ or angle
// brackets -- is quoted, so the attendee field splits it as one entry.
func formatICalEmailValue(value string, params map[string]string) string {
	email := strings.TrimSpace(value)
	if strings.HasPrefix(strings.ToLower(email), "mailto:") {
		email = email[7:]
	}
	name := params["CN"]
	if email == "" {
		return ""
	}
	if name == "" || strings.EqualFold(strings.TrimSpace(name), email) {
		return email
	}
	if strings.ContainsAny(name, "@<>") {
		name = `"` + name + `"`
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
