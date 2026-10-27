package ui

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
	"github.com/jw6ventures/calcard/internal/events"
	httperrors "github.com/jw6ventures/calcard/internal/http/errors"
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
		writeServerFailure(w, err, "failed to load calendars")
		return
	}
	users, err := h.store.Users.ListActive(r.Context())
	if err != nil {
		writeServerFailure(w, err, "failed to load users")
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
				writeServerFailure(w, err, "failed to load shares")
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
		if writeStoreUnavailable(w, err) {
			return
		}
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
		if writeStoreUnavailable(w, err) {
			return
		}
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
	// Only the name, and the colour when the form sends one, are written, as a
	// PROPPATCH of them would write them, under a lock on the calendar or a
	// depth-infinity lock above it. A property a client changed since this
	// handler read the calendar is kept.
	_, setColor := r.Form["color"]
	err = h.store.RenameCalendarAndState(r.Context(), user.ID, id, name, color, setColor, calendarWriteLockPreconditions(id))
	if errors.Is(err, store.ErrLockConflict) {
		h.redirect(w, r, "/calendars", map[string]string{"error": calendarLockedMessage})
		return
	}
	if err != nil {
		if writeStoreUnavailable(w, err) {
			return
		}
		httperrors.LogError(r, "rename calendar", err)
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

// DeleteCalendar deletes a calendar, with its events and the DAV state bound
// beneath it, as a CalDAV DELETE of the collection does: it is refused, with
// nothing removed, while a lock holds the calendar or any event in it.
func (h *Handler) DeleteCalendar(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		h.redirect(w, r, "/calendars", map[string]string{"error": "invalid id"})
		return
	}
	for attempt := 1; ; attempt++ {
		if !h.deleteCalendarOnce(w, r, id) {
			return
		}
		if attempt == maxEventWriteAttempts {
			h.redirect(w, r, "/calendars", map[string]string{"error": "This calendar kept changing elsewhere and could not be deleted. Reload the page and try again."})
			return
		}
	}
}

// deleteCalendarOnce reads the calendar and its events and deletes exactly
// them. It reports true, having answered nothing, when the calendar or an event
// changed after they were read, which the caller reads again.
func (h *Handler) deleteCalendarOnce(w http.ResponseWriter, r *http.Request, id int64) (changedConcurrently bool) {
	user, _ := auth.UserFromContext(r.Context())
	cal, err := h.store.Calendars.GetByID(r.Context(), id)
	if err != nil {
		if writeStoreUnavailable(w, err) {
			return false
		}
		httperrors.LogError(r, "delete calendar", err)
		h.redirect(w, r, "/calendars", map[string]string{"error": "delete failed"})
		return false
	}
	if cal == nil || cal.UserID != user.ID {
		h.redirect(w, r, "/calendars", map[string]string{"error": "delete failed"})
		return false
	}
	events, err := h.store.Events.ListForCalendar(r.Context(), id)
	if err != nil {
		if writeStoreUnavailable(w, err) {
			return false
		}
		httperrors.LogError(r, "delete calendar", err)
		h.redirect(w, r, "/calendars", map[string]string{"error": "delete failed"})
		return false
	}
	members := make([]store.CalendarCollectionMember, 0, len(events))
	for _, event := range events {
		members = append(members, store.CalendarCollectionMember{
			ID: event.ID, UID: event.UID, ResourceName: calendarEventResourceName(event.UID, &event), RawICAL: event.RawICAL, ETag: event.ETag,
		})
	}
	statePath := calendarACLResourcePath(id)
	_, err = h.store.TransferCalendarCollection(r.Context(), store.CalendarCollectionTransfer{
		Operation:          store.CalendarCollectionDelete,
		SourceID:           id,
		ExpectedSourceCTag: cal.CTag,
		SourceStatePath:    statePath,
		Members:            members,
		LockPreconditions:  []store.LockPrecondition{store.NewLockPrecondition(statePath), store.NewLockPrecondition(path.Dir(statePath))},
		RevalidateMembers:  store.RemovedCalendarCollectionMembers,
	})
	switch {
	case err == nil:
		h.redirect(w, r, "/calendars", map[string]string{"status": "calendar_deleted"})
		return false
	case errors.Is(err, store.ErrResourceStateChanged):
		return true
	case errors.Is(err, store.ErrLockConflict):
		h.redirect(w, r, "/calendars", map[string]string{"error": calendarDeleteLockedMessage})
		return false
	default:
		if writeStoreUnavailable(w, err) {
			return false
		}
		httperrors.LogError(r, "delete calendar", err)
		h.redirect(w, r, "/calendars", map[string]string{"error": "delete failed"})
		return false
	}
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
		if writeStoreUnavailable(w, err) {
			return
		}
		h.redirect(w, r, "/calendars", map[string]string{"error": "not found"})
		return
	}

	targetUser, err := h.store.Users.GetByID(r.Context(), targetID)
	if err != nil || targetUser == nil {
		if writeStoreUnavailable(w, err) {
			return
		}
		h.redirect(w, r, "/calendars", map[string]string{"error": "user not found"})
		return
	}

	if err := h.setCalendarShare(r.Context(), cal.ID, targetUser.ID, true); err != nil {
		if writeStoreUnavailable(w, err) {
			return
		}
		if errors.Is(err, store.ErrLockConflict) {
			h.redirect(w, r, "/calendars", map[string]string{"error": calendarLockedMessage})
			return
		}
		if errors.Is(err, store.ErrNotFound) {
			h.redirect(w, r, "/calendars", map[string]string{"error": "not found"})
			return
		}
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
		if writeStoreUnavailable(w, err) {
			return
		}
		h.redirect(w, r, "/calendars", map[string]string{"error": "not found"})
		return
	}

	if calAccess.UserID == user.ID {
		// Owner removing a share
		if err := h.removeCalendarShare(r.Context(), calendarID, targetID); err != nil {
			if writeStoreUnavailable(w, err) {
				return
			}
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
			if writeStoreUnavailable(w, err) {
				return
			}
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

// calendarEventResourcePath is the state path of the event resourceName
// names, where DAV reads and writes its ACL.
func calendarEventResourcePath(calendarID int64, resourceName string) string {
	return store.MemberStatePath(calendarACLResourcePath(calendarID), resourceName)
}

// calendarEventResourceName is the stored name of existing, which is its UID
// where it records none, or the name an import of uid stores a new event
// under.
func calendarEventResourceName(uid string, existing *store.Event) string {
	switch {
	case existing == nil:
		return utils.ResourceNameForUID(uid)
	case existing.ResourceName != "":
		return existing.ResourceName
	default:
		return existing.UID
	}
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

// decideCalendarPrivilege evaluates the resource's ordered ACEs, those at its
// state path, before falling back to the collection's ordered ACEs.
func decideCalendarPrivilege(user *store.User, cal *store.CalendarAccess, resourcePath, privilege string, lookup func(candidate string) ([]store.ACLEntry, error)) (bool, error) {
	if cal == nil || user == nil {
		return false, nil
	}
	if cal.UserID == user.ID {
		return true, nil
	}

	collectionPath := calendarACLResourcePath(cal.ID)
	var resourceEntries []store.ACLEntry
	if resourcePath != collectionPath {
		entries, err := lookup(resourcePath)
		if err != nil {
			return false, err
		}
		resourceEntries = slices.Clone(entries)
		acl.SortEntries(resourceEntries)
	}
	collectionEntries, err := lookup(collectionPath)
	if err != nil {
		return false, err
	}
	collectionEntries = slices.Clone(collectionEntries)
	acl.SortEntries(collectionEntries)
	granted, _ := acl.DecisionThroughLayers(acl.ApplicablePrincipals(user), privilege, resourceEntries, collectionEntries)
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

// invalidEventMessage is the flash message for calendar data the validator
// refused: what did not happen, then why, in words for the user rather than
// the RFC 4791 precondition names a DAV client is answered with.
func invalidEventMessage(action string, err error) string {
	const fallback = "the server could not accept its calendar data"
	var refusal *calobject.Error
	if !errors.As(err, &refusal) || len(refusal.Conditions) == 0 {
		return action + ": " + fallback
	}
	if errors.Is(refusal, calobject.ErrUIDTooLong) {
		return action + ": its UID is longer than this server accepts"
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
	relevantPaths := make([]string, 0, 1+len(events))
	relevantPaths = append(relevantPaths, calendarACLResourcePath(calendarID))
	for _, event := range events {
		relevantPaths = append(relevantPaths, calendarEventResourcePath(calendarID, calendarEventResourceName(event.UID, &event)))
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
		if !entry.IsGrant || !calendarShareVisiblePrivilege(entry.Privilege) {
			continue
		}
		userID, ok := acl.PrincipalUserID(entry.PrincipalHref)
		if !ok {
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
	return h.store.UpdateACLAndState(ctx, resourcePath, calendarWriteLockPreconditions(calendarID), func(entries []store.ACLEntry) ([]store.ACLEntry, error) {
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
// access, needs no access precondition whoever asks for it, and is refused by
// no lock: a principal who locked the calendar could otherwise not be removed.
// The locks the principal holds on the calendar and its events go with the
// grants.
func (h *Handler) removeCalendarShare(ctx context.Context, calendarID, userID int64) error {
	return h.store.ACLEntries.RevokePrincipalGrants(ctx, calendarACLResourcePath(calendarID), calendarSharePrincipalHref(userID), calendarShareManagedPrivileges)
}

// calendarWriteLockPreconditions are the lock conditions a write to the
// calendar's own properties or ACL meets without a lock token: a lock on the
// calendar at any depth, or a depth-infinity lock above it, refuses it, and a
// lock on one of its events does not.
func calendarWriteLockPreconditions(calendarID int64) []store.LockPrecondition {
	return []store.LockPrecondition{store.NewLockPrecondition(calendarACLResourcePath(calendarID))}
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
		writeServerFailure(w, err, "failed to load calendar")
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
		writeQueryRangeError(w, r, err)
		return
	}

	user, _ := auth.UserFromContext(r.Context())
	cal, err := h.store.Calendars.GetAccessible(r.Context(), id, user.ID)
	if err != nil {
		writeServerFailure(w, err, "failed to load calendar")
		return
	}
	if cal == nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	// Fetch all events for the calendar
	allEvents, err := h.store.Events.ListForCalendar(r.Context(), id)
	if err != nil {
		writeServerFailure(w, err, "failed to load events")
		return
	}
	allEvents, err = h.filterReadableCalendarEvents(r.Context(), user, cal, allEvents)
	if err != nil {
		writeServerFailure(w, err, "failed to evaluate event access")
		return
	}

	// Filter events relevant to the requested range (end is inclusive here).
	rangeEnd := rangeEndExclusive.Add(-time.Nanosecond)
	relevantEvents := filterEventsForMonth(allEvents, rangeStart, rangeEnd)

	// Build JSON response
	viewStart, viewEnd := eventViewWindow(rangeStart, rangeEndExclusive)
	work := ical.NewRequestWork(r.Context(), eventsJSONWorkBudget)
	eventsJSONData := make([]map[string]any, 0, len(relevantEvents))
	for _, ev := range relevantEvents {
		if r.Context().Err() != nil {
			return
		}
		eventWork := work.Allowance(eventsJSONEventWork)
		payload := unfoldedEventJSON(ev, viewStart, viewEnd, h.timezoneOnsets, eventWork)
		h.addSeriesInstances(payload, ev, viewStart, viewEnd, eventWork)
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
		writeServerFailure(w, err, "failed to load calendars")
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
			writeServerFailure(w, err, "failed to evaluate calendar access")
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

// eventsJSONWorkBudget bounds the recurrence and observance work expanding
// every series one events.json response lists may spend together, on top of
// the cap each evaluation has of its own and of each event's allowance. It is
// the size of a REPORT's: thirty series in zones that each cost several million
// units to follow are answered in about two seconds with it.
const eventsJSONWorkBudget = 20_000_000

// eventsJSONEventWork is the allowance each event an events.json response
// lists keeps beside the shared eventsJSONWorkBudget: the work its zone reading
// and expansion may spend whether or not the events listed before it have spent
// the shared budget. The browser places a series by the instances it is served,
// so a series served none is not shown. Measured on a month view, a weekly
// UTC series costs 15 units, a weekly series in Outlook's "Eastern Standard
// Time" 18 once the zone's rules are listed and 5,400 when its zone reading and
// expansion list them, and an hourly series of 770 instances there 4,200 cold.
// 10,000 leaves the costliest of them nearly twice its cost, and the shared
// budget serves whatever a series needs beyond it. A response of hostile events
// costs the shared budget and this many units for each event listed.
const eventsJSONEventWork = 10_000

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
// "instances", which the browser places the series by, "instancesComplete",
// false when the range holds more than it lists, and "overrideInstances",
// where each override replacing one instance falls. An event whose master
// does not recur, or whose recurrence the server cannot expand, gets none of
// them, and stands for itself.
func (h *Handler) addSeriesInstances(payload map[string]any, unfolded unfoldedEvent, viewStart, viewEnd time.Time, work *ical.RequestWork) {
	if !unfolded.series.recurs() {
		return
	}
	expansion := h.expandEvent(unfolded.event.RawICAL, viewStart, viewEnd, work)
	if !expansion.Recurs {
		return
	}
	payload["instances"] = viewInstances(expansion.Instances)
	payload["instancesComplete"] = expansion.Complete
	if len(expansion.Overrides) > 0 {
		payload["overrideInstances"] = viewInstances(expansion.Overrides)
	}
}

func viewInstances(instances []calobject.Instance) []calendarEventInstance {
	listed := make([]calendarEventInstance, 0, len(instances))
	for _, instance := range instances {
		view := calendarEventInstance{
			RecurrenceID: instance.RecurrenceID.UTC().Format(time.RFC3339),
			Start:        instance.Start.UTC().Format(time.RFC3339),
			End:          instance.End.UTC().Format(time.RFC3339),
		}
		if !instance.Override.IsZero() {
			view.Override = instance.Override.UTC().Format(time.RFC3339)
		}
		listed = append(listed, view)
	}
	return listed
}

// calendarEventQueryRange resolves the [start, endExclusive) window an events.json
// request targets. It accepts an explicit start/end date range (used by the day/week
// views) and falls back to a whole-month window derived from year/month.
func calendarEventQueryRange(r *http.Request, now time.Time) (time.Time, time.Time, error) {
	startValue := strings.TrimSpace(r.URL.Query().Get("start"))
	endValue := strings.TrimSpace(r.URL.Query().Get("end"))
	if startValue != "" || endValue != "" {
		if startValue == "" || endValue == "" {
			return time.Time{}, time.Time{}, &queryRangeError{message: "start and end are required"}
		}
		start, err := time.Parse("2006-01-02", startValue)
		if err != nil {
			return time.Time{}, time.Time{}, &queryRangeError{message: "invalid start date"}
		}
		endExclusive, err := time.Parse("2006-01-02", endValue)
		if err != nil {
			return time.Time{}, time.Time{}, &queryRangeError{message: "invalid end date"}
		}
		if !endExclusive.After(start) || endExclusive.Sub(start) > maxAllCalendarRangeDays*24*time.Hour {
			return time.Time{}, time.Time{}, &queryRangeError{message: "invalid date range"}
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

// queryRangeError is a refusal of the date range an events listing was asked
// for, with the plain message it is answered with.
type queryRangeError struct{ message string }

func (e *queryRangeError) Error() string { return e.message }

// writeQueryRangeError answers a request for a date range calendarEventQueryRange
// refused with its plain message, and any other failure with a generic one,
// keeping the detail in the log.
func writeQueryRangeError(w http.ResponseWriter, r *http.Request, err error) {
	var refusal *queryRangeError
	if errors.As(err, &refusal) {
		http.Error(w, refusal.message, http.StatusBadRequest)
		return
	}
	httperrors.LogError(r, "events date range", err)
	http.Error(w, "invalid date range", http.StatusBadRequest)
}

// GetAllCalendarEventsJSON returns events across all accessible calendars for a date range.
func (h *Handler) GetAllCalendarEventsJSON(w http.ResponseWriter, r *http.Request) {
	rangeStart, rangeEndExclusive, err := calendarEventQueryRange(r, time.Now())
	if err != nil {
		writeQueryRangeError(w, r, err)
		return
	}

	user, _ := auth.UserFromContext(r.Context())
	calendars, err := h.store.Calendars.ListAccessible(r.Context(), user.ID)
	if err != nil {
		writeServerFailure(w, err, "failed to load calendars")
		return
	}

	rangeEnd := rangeEndExclusive.Add(-time.Nanosecond)
	viewStart, viewEnd := eventViewWindow(rangeStart, rangeEndExclusive)

	work := ical.NewRequestWork(r.Context(), eventsJSONWorkBudget)
	var result = make([]map[string]any, 0)
	for i, cal := range calendars {
		if r.Context().Err() != nil {
			return
		}
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
			writeServerFailure(w, err, "failed to evaluate event access")
			return
		}

		color := calendarColor(cal.Calendar.Color, i)
		for _, unfoldedEv := range unfolded {
			ev := unfoldedEv.event
			resourcePath := calendarEventResourcePath(cal.ID, calendarEventResourceName(ev.UID, &ev))
			if !decide(resourcePath, "read") {
				continue
			}

			eventWork := work.Allowance(eventsJSONEventWork)
			payload := unfoldedEventJSON(unfoldedEv, viewStart, viewEnd, h.timezoneOnsets, eventWork)
			h.addSeriesInstances(payload, unfoldedEv, viewStart, viewEnd, eventWork)
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
		writeServerFailure(w, err, "failed to load calendar")
		return
	}
	if cal == nil {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if err := h.requireCalendarPrivilege(r.Context(), user, cal, calendarACLResourcePath(calendarID), "read"); err != nil {
		if writeStoreUnavailable(w, err) {
			return
		}
		http.Error(w, "forbidden", http.StatusForbidden)
		return
	}

	events, err := h.store.Events.ListForCalendar(r.Context(), calendarID)
	if err != nil {
		writeServerFailure(w, err, "failed to load events")
		return
	}
	events, err = h.filterReadableCalendarEvents(r.Context(), user, cal, events)
	if err != nil {
		writeServerFailure(w, err, "failed to evaluate event access")
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
	entries, err := h.store.ACLEntries.ListByPrincipals(ctx, calendarApplicablePrincipalHrefs(user))
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
	return importForbidden, nil
}

// importTarget is the stored event an import of uid replaces, if any, and
// whether the caller may write it: bind on the calendar for a new event,
// which RFC 3744 section 3.9 makes a privilege of the collection, and
// write-content on the event an existing one is. The guard pins the ACL
// entries the decision reads, for the write to carry.
func (h *Handler) importTarget(ctx context.Context, user *store.User, cal *store.CalendarAccess, uid string) (existing *store.Event, guard *store.ACLGuard, allowed bool, err error) {
	existing, err = h.store.Events.GetByUID(ctx, cal.ID, uid)
	if err != nil {
		return nil, nil, false, err
	}
	resourceName := ""
	if existing != nil {
		resourceName = calendarEventResourceName(uid, existing)
	}
	guard, err = h.pinEventACL(ctx, user, eventACLScope{calendarID: cal.ID, calendar: &cal.Calendar, resourceName: resourceName})
	if err != nil {
		return nil, nil, false, err
	}
	if existing == nil {
		allowed, err = h.hasCalendarPrivilege(ctx, user, cal, calendarACLResourcePath(cal.ID), "bind")
		return nil, guard, allowed, err
	}
	allowed, err = h.hasCalendarPrivilege(ctx, user, cal, calendarEventResourcePath(cal.ID, resourceName), "write-content")
	return existing, guard, allowed, err
}

// importItem is an event to import with the version of the stored event the
// decision to write it was made on, nil for a new event, and the ACL entries
// that decision read.
type importItem struct {
	event    store.Event
	existing *store.Event
	guard    *store.ACLGuard
}

// importOutcome is what became of one event of an import.
type importOutcome int

const (
	importWritten importOutcome = iota
	importFailed
	// importLocked: a lock a CalDAV client holds refused the write.
	importLocked
	// importChanged: the event or the access that decided its write changed
	// before it was written.
	importChanged
	// importUnavailable: the store could not be reached, so the rest of the
	// import cannot be written either.
	importUnavailable
)

// writeImportedEvent writes one event of an import as a UI create or edit is
// written: only over the version it was decided on, only while the ACL it was
// decided on holds, and only past the locks a CalDAV request submitting no
// token would meet. A write that loses to another writer is decided again: it
// counts as written when the caller may write the event and finds it already
// holding the octets the import would store, as when another import of the
// same file wins the race. A write that loses to a change of the ACL alone
// goes on, once, when that finds the event as it was.
func (h *Handler) writeImportedEvent(ctx context.Context, r *http.Request, user *store.User, cal *store.CalendarAccess, item importItem) importOutcome {
	for attempt := 1; ; attempt++ {
		var err error
		if item.existing == nil {
			err = h.storeNewEvent(ctx, item.event, item.guard)
		} else {
			err = h.storeEventEdit(ctx, item.existing, item.event, item.guard)
		}
		switch {
		case err == nil:
			return importWritten
		case errors.Is(err, store.ErrLockConflict):
			return importLocked
		case errors.Is(err, store.ErrPreconditionFailed), errors.Is(err, store.ErrUIDConflict), errors.Is(err, store.ErrResourceStateChanged):
			existing, guard, allowed, decideErr := h.importTarget(ctx, user, cal, item.event.UID)
			if decideErr != nil {
				return importFailure(r, decideErr)
			}
			if allowed && holdsImportedContent(existing, item.event) {
				return importWritten
			}
			aclChangedAlone := errors.Is(err, store.ErrResourceStateChanged) && sameStoredVersion(existing, item.existing)
			if attempt == 2 || !allowed || !aclChangedAlone {
				return importChanged
			}
			item.guard = guard
		default:
			return importFailure(r, err)
		}
	}
}

// holdsImportedContent reports whether stored is the event an import of event
// would leave: the same UID holding the same calendar data.
func holdsImportedContent(stored *store.Event, event store.Event) bool {
	return stored != nil && stored.UID == event.UID && stored.RawICAL == event.RawICAL
}

// importFailure is the outcome of an event whose write failed with err.
func importFailure(r *http.Request, err error) importOutcome {
	if store.IsUnavailable(err) {
		return importUnavailable
	}
	httperrors.LogError(r, "import event", err)
	return importFailed
}

// sameStoredVersion reports whether a and b are the same version of one event,
// or both absent.
func sameStoredVersion(a, b *store.Event) bool {
	if a == nil || b == nil {
		return a == b
	}
	return a.UID == b.UID && a.ETag == b.ETag
}

// unclaimedImportName is the name a new event uid is imported under: the one
// utils.ResourceNameForUID derives from uid, or, where an event of another UID
// is stored under that name or claims it earlier in the same import, that name
// with a suffix derived from the whole UID, since distinct UIDs can derive one
// name.
func (h *Handler) unclaimedImportName(ctx context.Context, calendarID int64, uid string, claimed map[string]struct{}) (string, error) {
	name := utils.ResourceNameForUID(uid)
	digest := sha256.Sum256([]byte(uid))
	suffix := "-" + hex.EncodeToString(digest[:6])
	suffixed := name[:min(len(name), store.MaxIdentifierOctets-len(suffix))] + suffix
	for _, candidate := range []string{name, suffixed} {
		if _, taken := claimed[candidate]; taken {
			continue
		}
		holder, err := h.store.Events.GetByResourceName(ctx, calendarID, candidate)
		if err != nil {
			return "", err
		}
		if holder == nil {
			return candidate, nil
		}
	}
	return utils.ResourceNameForUID(utils.GenerateUID()), nil
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
		writeServerFailure(w, err, "failed to load calendar")
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
		writeServerFailure(w, err, "failed to check calendar access")
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
		httperrors.LogError(r, "import calendar", err)
		h.redirect(w, r, fmt.Sprintf("/calendars/%d", calendarID), map[string]string{"error": "The file is not a valid iCalendar file with at least one event."})
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
	pending := make([]importItem, 0, len(events))
	claimed := make(map[string]struct{}, len(events))
	for _, eventICAL := range events {
		uid := utils.ExtractUID(eventICAL)
		if uid == "" {
			uid = utils.GenerateUID()
			eventICAL = utils.EnsureUID(eventICAL, uid)
		}
		var existing *store.Event
		var guard *store.ACLGuard
		decided := false
		if access == importPerEvent {
			allowed := false
			existing, guard, allowed, err = h.importTarget(r.Context(), user, cal, uid)
			if err != nil {
				writeServerFailure(w, err, "failed to load event")
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
			existing, guard, allowed, err = h.importTarget(r.Context(), user, cal, uid)
			if err != nil {
				writeServerFailure(w, err, "failed to load event")
				return
			}
			if !allowed {
				http.Error(w, "forbidden", http.StatusForbidden)
				return
			}
		}
		event.ResourceName = calendarEventResourceName(uid, existing)
		if existing != nil && !store.ValidMemberName(event.ResourceName) {
			refused++
			continue
		}
		if existing == nil {
			if event.ResourceName, err = h.unclaimedImportName(r.Context(), cal.ID, uid, claimed); err != nil {
				writeServerFailure(w, err, "failed to load event")
				return
			}
		}
		claimed[event.ResourceName] = struct{}{}
		pending = append(pending, importItem{event: event, existing: existing, guard: guard})
	}

	// The decisions above are all made before the first write, so a forbidden
	// event stops a collection-wide import before anything is stored. The writes
	// are separate, each atomic on its own: one that fails or is refused leaves
	// the others to go on.
	counts := map[importOutcome]int{}
	for _, item := range pending {
		outcome := h.writeImportedEvent(r.Context(), r, user, cal, item)
		if outcome == importUnavailable {
			writeServiceUnavailable(w)
			return
		}
		counts[outcome]++
	}
	imported, failed := counts[importWritten], counts[importFailed]

	var skippedParts []string
	if refused > 0 {
		skippedParts = append(skippedParts, fmt.Sprintf("skipped %d (not valid or not events)", refused))
	}
	if locked := counts[importLocked]; locked > 0 {
		skippedParts = append(skippedParts, fmt.Sprintf("skipped %d held by a lock of a CalDAV client", locked))
	}
	if changed := counts[importChanged]; changed > 0 {
		skippedParts = append(skippedParts, fmt.Sprintf("skipped %d changed during the import", changed))
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

	for attempt := 1; ; attempt++ {
		if !h.createEventOnce(w, r, calendarID) {
			return
		}
		if attempt == maxEventWriteAttempts {
			h.redirectToEventPage(w, r, calendarID, map[string]string{"error": eventKeptChangingMessage("saved")})
			return
		}
	}
}

// createEventOnce decides and writes one create. It reports true, having
// answered nothing, when the write lost to a change of the calendar's access
// that was made after the decision, which the caller decides again.
func (h *Handler) createEventOnce(w http.ResponseWriter, r *http.Request, calendarID int64) (changedConcurrently bool) {
	user, _ := auth.UserFromContext(r.Context())
	cal, err := h.store.Calendars.GetAccessible(r.Context(), calendarID, user.ID)
	if err != nil {
		writeServerFailure(w, err, "failed to load calendar")
		return false
	}
	if cal == nil {
		http.Error(w, "not found", http.StatusNotFound)
		return false
	}

	summary := strings.TrimSpace(r.FormValue("summary"))
	if summary == "" {
		h.redirectToEventPage(w, r, calendarID, map[string]string{"error": "summary is required"})
		return false
	}

	dtstart := strings.TrimSpace(r.FormValue("dtstart"))
	dtend := strings.TrimSpace(r.FormValue("dtend"))
	if dtstart == "" || dtend == "" {
		h.redirectToEventPage(w, r, calendarID, map[string]string{"error": "start and end are required"})
		return false
	}

	allDay := r.FormValue("all_day") == "on"
	opts := parseEventOptions(r)

	// Validate date format and range
	if err := utils.ValidateEventDates(dtstart, dtend, allDay, utils.EventZoneIn(opts.Timezone, nil)); err != nil {
		h.redirectToEventPage(w, r, calendarID, map[string]string{"error": eventFormFlash(r, err)})
		return false
	}

	location := strings.TrimSpace(r.FormValue("location"))
	description := strings.TrimSpace(r.FormValue("description"))

	recurrence, err := utils.ParseRecurrenceOptions(r)
	if err != nil {
		h.redirectToEventPage(w, r, calendarID, map[string]string{"error": eventFormFlash(r, err)})
		return false
	}

	uid := utils.GenerateUID()
	guard, err := h.pinEventACL(r.Context(), user, eventACLScope{calendarID: calendarID, calendar: &cal.Calendar})
	if err != nil {
		writeServerFailure(w, err, "failed to evaluate event access")
		return false
	}
	// DAV:bind is a privilege of the calendar the event joins (RFC 3744
	// section 3.9), so no ACL stored for a member bears on it.
	if err := h.requireCalendarPrivilege(r.Context(), user, cal, calendarACLResourcePath(calendarID), "bind"); err != nil {
		if writeStoreUnavailable(w, err) {
			return false
		}
		http.Error(w, "forbidden", http.StatusForbidden)
		return false
	}
	if !calendarAcceptsUIWrite(cal) {
		h.redirectToEventPage(w, r, calendarID, map[string]string{"error": "this calendar does not accept events"})
		return false
	}
	body, err := utils.BuildEvent(uid, summary, dtstart, dtend, allDay, location, description, recurrence, opts)
	if err != nil {
		h.redirectToEventPage(w, r, calendarID, map[string]string{"error": eventFormFlash(r, err)})
		return false
	}
	event, err := h.validatedEvent(cal, uid, body)
	if err != nil {
		h.redirectToEventPage(w, r, calendarID, map[string]string{"error": invalidEventMessage(eventNotSaved, err)})
		return false
	}

	err = h.storeNewEvent(r.Context(), event, guard)
	switch {
	case errors.Is(err, store.ErrResourceStateChanged):
		return true
	case errors.Is(err, store.ErrLockConflict):
		h.redirectToEventPage(w, r, calendarID, map[string]string{"error": eventLockedMessage})
		return false
	case err != nil:
		if writeStoreUnavailable(w, err) {
			return false
		}
		httperrors.LogError(r, "failed to create event", err)
		h.redirectToEventPage(w, r, calendarID, map[string]string{"error": "failed to create event"})
		return false
	}

	h.redirectToEventPage(w, r, calendarID, map[string]string{"status": "event_created"})
	return false
}

// UpdateEvent updates an existing event. The form names the version of the
// event it was built from, and the update applies only to that version.
func (h *Handler) UpdateEvent(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}

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

	for attempt := 1; ; attempt++ {
		if !h.updateEventOnce(w, r, calendarID, uid) {
			return
		}
		if attempt == maxEventWriteAttempts {
			h.redirectToEventPage(w, r, calendarID, map[string]string{"error": eventKeptChangingMessage("saved")})
			return
		}
	}
}

// updateEventOnce decides and writes one update. It reports true, having
// answered nothing, when the write lost to a change of the event or of the
// access that decided it, made after the decision, which the caller decides
// again from the event as it then is.
func (h *Handler) updateEventOnce(w http.ResponseWriter, r *http.Request, calendarID int64, uid string) (changedConcurrently bool) {
	editScope := r.FormValue("edit_scope")
	recurrenceID := strings.TrimSpace(r.FormValue("recurrence_id"))
	recurrenceAllDay := r.FormValue("recurrence_all_day") == "true"

	user, _ := auth.UserFromContext(r.Context())
	cal, err := h.store.Calendars.GetAccessible(r.Context(), calendarID, user.ID)
	if err != nil {
		writeServerFailure(w, err, "failed to load calendar")
		return false
	}
	if cal == nil {
		http.Error(w, "not found", http.StatusNotFound)
		return false
	}

	destinationCalendarID, validDestination := requestedDestination(r, calendarID)
	movingCalendar := validDestination && destinationCalendarID != calendarID
	var decidedAlso []eventACLScope
	if movingCalendar {
		decidedAlso = append(decidedAlso, eventACLScope{calendarID: destinationCalendarID})
	}
	existing, guard, ok := h.authorizedEventWrite(w, r, user, cal, uid, decidedAlso...)
	if !ok {
		return false
	}
	if !store.ValidMemberName(calendarEventResourceName(uid, existing)) {
		h.redirectToEventPage(w, r, calendarID, map[string]string{"error": unwritableEventMessage})
		return false
	}
	if !h.requireOpenedETag(w, r, calendarID, existing) {
		return false
	}

	if !validDestination {
		h.redirectToEventPage(w, r, calendarID, map[string]string{"error": "invalid destination calendar"})
		return false
	}
	if movingCalendar && editScope == "occurrence" {
		h.redirectToEventPage(w, r, calendarID, map[string]string{"error": "move the recurring series before editing one occurrence"})
		return false
	}

	resourceName := uid
	if existing.ResourceName != "" {
		resourceName = existing.ResourceName
	}
	destinationCalendar := cal
	if movingCalendar {
		if _, err := h.events.AuthorizeEventRemoval(r.Context(), user, calendarID, existing); err != nil {
			writeEventAccessError(w, err)
			return false
		}

		destinationCalendar, err = h.store.Calendars.GetAccessible(r.Context(), destinationCalendarID, user.ID)
		if err != nil {
			writeServerFailure(w, err, "failed to load destination calendar")
			return false
		}
		if destinationCalendar == nil {
			http.Error(w, "destination calendar not found", http.StatusNotFound)
			return false
		}
		// DAV:bind is a privilege of the calendar the event joins (RFC 3744
		// section 3.9), so no ACL stored for a member bears on it.
		if err := h.requireCalendarPrivilege(r.Context(), user, destinationCalendar, calendarACLResourcePath(destinationCalendarID), "bind"); err != nil {
			if writeStoreUnavailable(w, err) {
				return false
			}
			http.Error(w, "forbidden", http.StatusForbidden)
			return false
		}
		if !calendarAcceptsUIWrite(destinationCalendar) {
			h.redirectToEventPage(w, r, calendarID, map[string]string{"error": "destination calendar does not accept events"})
			return false
		}

		existingByUID, err := h.store.Events.GetByUID(r.Context(), destinationCalendarID, uid)
		if err != nil {
			writeServerFailure(w, err, "failed to check destination calendar")
			return false
		}
		existingByName, err := h.store.Events.GetByResourceName(r.Context(), destinationCalendarID, resourceName)
		if err != nil {
			writeServerFailure(w, err, "failed to check destination calendar")
			return false
		}
		if existingByUID != nil || existingByName != nil {
			h.redirectToEventPage(w, r, calendarID, map[string]string{"error": "destination calendar already contains this event"})
			return false
		}
	}

	summary := strings.TrimSpace(r.FormValue("summary"))
	if summary == "" {
		h.redirectToEventPage(w, r, calendarID, map[string]string{"error": "summary is required"})
		return false
	}

	dtstart := strings.TrimSpace(r.FormValue("dtstart"))
	dtend := strings.TrimSpace(r.FormValue("dtend"))
	if dtstart == "" || dtend == "" {
		h.redirectToEventPage(w, r, calendarID, map[string]string{"error": "start and end are required"})
		return false
	}

	allDay := r.FormValue("all_day") == "on"
	opts := parseEventOptions(r)
	opts.Object = withoutServerZoneDefinitions(utils.UnfoldLines(existing.RawICAL))
	opts.EditorAddresses = editorAddresses(user)

	// Validate date format and range
	if err := utils.ValidateEventDates(dtstart, dtend, allDay, utils.EventZoneIn(opts.Timezone, opts.Object)); err != nil {
		h.redirectToEventPage(w, r, calendarID, map[string]string{"error": eventFormFlash(r, err)})
		return false
	}

	location := strings.TrimSpace(r.FormValue("location"))
	description := strings.TrimSpace(r.FormValue("description"))

	recurrence, err := utils.ParseRecurrenceOptions(r)
	if err != nil {
		h.redirectToEventPage(w, r, calendarID, map[string]string{"error": eventFormFlash(r, err)})
		return false
	}

	header, components, footer := utils.SplitComponentLines(opts.Object)
	ical := ""
	if editScope == "occurrence" && recurrenceID != "" && existing.RawICAL != "" {
		// Update only a single occurrence by replacing/adding a RECURRENCE-ID component.
		override, err := utils.BuildEventComponent(uid, summary, dtstart, dtend, allDay, location, description, nil, "", opts)
		if err != nil {
			h.redirectToEventPage(w, r, calendarID, map[string]string{"error": eventFormFlash(r, err)})
			return false
		}
		recLine, err := utils.FormatICalDateTimeIn(recurrenceID, recurrenceAllDay, false, "RECURRENCE-ID", occurrenceZone(r, opts.Object))
		if err != nil {
			h.redirectToEventPage(w, r, calendarID, map[string]string{"error": "invalid occurrence date"})
			return false
		}
		if recLine != "" {
			if len(override) >= 2 {
				override = append(override[:2], append([]string{recLine}, override[2:]...)...)
			} else {
				override = append([]string{recLine}, override...)
			}
		}

		var newComponents [][]string
		for _, comp := range components {
			if !utils.SameRecurrenceID(comp, override) {
				newComponents = append(newComponents, comp)
			}
		}
		if source := h.overrideSourceOf(existing.RawICAL, opts.Object, components, override); source.previous != nil {
			override = withUneditedOverrideProperties(override, source, opts)
		}
		newComponents = append(newComponents, override)
		ical = utils.BuildFromComponents(header, newComponents, footer)
	} else {
		// Update the series/master event while keeping overrides intact.
		master, err := utils.BuildEventComponent(uid, summary, dtstart, dtend, allDay, location, description, recurrence, "", opts)
		if err != nil {
			h.redirectToEventPage(w, r, calendarID, map[string]string{"error": eventFormFlash(r, err)})
			return false
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
		return false
	}
	if movingCalendar {
		fromPath := calendarEventResourcePath(calendarID, resourceName)
		toPath := calendarEventResourcePath(destinationCalendarID, resourceName)
		err = h.store.UpdateAndMoveEventAndState(r.Context(), calendarID, expectedEventState(existing, guard), updatedEvent, fromPath, toPath, moveLockPreconditions(calendarID, destinationCalendarID, resourceName))
	} else {
		err = h.storeEventEdit(r.Context(), existing, updatedEvent, guard)
	}
	switch {
	case err == nil:
	case errors.Is(err, store.ErrResourceStateChanged):
		return true
	case errors.Is(err, store.ErrPreconditionFailed):
		h.redirectToEventPage(w, r, calendarID, map[string]string{"error": eventChangedElsewhereMessage})
		return false
	case errors.Is(err, store.ErrLockConflict):
		message := eventLockedMessage
		if movingCalendar {
			message = eventMoveLockedMessage
		}
		h.redirectToEventPage(w, r, calendarID, map[string]string{"error": message})
		return false
	case movingCalendar && errors.Is(err, store.ErrConflict):
		h.redirectToEventPage(w, r, calendarID, map[string]string{"error": "destination calendar already contains this event"})
		return false
	default:
		if writeStoreUnavailable(w, err) {
			return false
		}
		httperrors.LogError(r, "failed to update event", err)
		h.redirectToEventPage(w, r, calendarID, map[string]string{"error": "failed to update event"})
		return false
	}

	h.redirectToEventPage(w, r, calendarID, map[string]string{"status": "event_updated"})
	return false
}

// DeleteEvent removes an event from a calendar, or one occurrence of it. The
// form names the version of the event it was built from, and the removal
// applies only to that version.
func (h *Handler) DeleteEvent(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseForm()

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

	for attempt := 1; ; attempt++ {
		if !h.deleteEventOnce(w, r, calendarID, uid) {
			return
		}
		if attempt == maxEventWriteAttempts {
			h.redirectToEventPage(w, r, calendarID, map[string]string{"error": eventKeptChangingMessage("deleted")})
			return
		}
	}
}

// deleteEventOnce decides and carries out one removal. It reports true,
// having answered nothing, when the write lost to a change of the event or of
// the access that decided it, made after the decision, which the caller
// decides again from the event as it then is.
func (h *Handler) deleteEventOnce(w http.ResponseWriter, r *http.Request, calendarID int64, uid string) (changedConcurrently bool) {
	editScope := r.FormValue("edit_scope")
	recurrenceID := strings.TrimSpace(r.FormValue("recurrence_id"))
	recurrenceAllDay := r.FormValue("recurrence_all_day") == "true"

	user, _ := auth.UserFromContext(r.Context())
	cal, err := h.store.Calendars.GetAccessible(r.Context(), calendarID, user.ID)
	if err != nil {
		writeServerFailure(w, err, "failed to load calendar")
		return false
	}
	if cal == nil {
		http.Error(w, "not found", http.StatusNotFound)
		return false
	}

	if editScope == "occurrence" && recurrenceID != "" {
		existing, guard, ok := h.authorizedEventWrite(w, r, user, cal, uid)
		if !ok {
			return false
		}
		if !store.ValidMemberName(calendarEventResourceName(uid, existing)) {
			h.redirectToEventPage(w, r, calendarID, map[string]string{"error": unwritableEventMessage})
			return false
		}
		if !h.requireOpenedETag(w, r, calendarID, existing) {
			return false
		}

		// The occurrence is removed as an edit of it is stored: from the object
		// less the definitions the server wrote, read in the host's zones,
		// whose definitions the validator stores afresh.
		object := withoutServerZoneDefinitions(utils.UnfoldLines(existing.RawICAL))
		exdateLine, err := utils.FormatICalDateTimeIn(recurrenceID, recurrenceAllDay, false, "EXDATE", occurrenceZone(r, object))
		if err != nil || exdateLine == "" {
			h.redirectToEventPage(w, r, calendarID, map[string]string{"error": "invalid recurrence id"})
			return false
		}
		header, components, footer := utils.SplitComponentLines(object)
		newComponents, masterHandled := h.withoutOccurrence(existing.RawICAL, object, components, exdateLine, editorAddresses(user))

		if !masterHandled {
			// No master to update; fall back to deleting the whole event.
			if _, err := h.events.AuthorizeEventRemoval(r.Context(), user, calendarID, existing); err != nil {
				writeEventAccessError(w, err)
				return false
			}
			resourcePath := calendarEventResourcePath(calendarID, calendarEventResourceName(uid, existing))
			return h.deleteEventState(w, r, calendarID, expectedEventState(existing, guard), resourcePath)
		}

		resourceName := uid
		if existing.ResourceName != "" {
			resourceName = existing.ResourceName
		}
		updatedEvent, err := h.validatedEvent(cal, resourceName, utils.BuildFromComponents(header, newComponents, footer))
		if err != nil {
			message := invalidEventMessage("the occurrence could not be deleted", err)
			if _, storedErr := h.validateCalendarObject(existing.RawICAL, cal); storedErr != nil {
				message += "; the whole series can still be deleted"
			}
			h.redirectToEventPage(w, r, calendarID, map[string]string{"error": message})
			return false
		}
		err = h.storeEventEdit(r.Context(), existing, updatedEvent, guard)
		switch {
		case err == nil:
		case errors.Is(err, store.ErrResourceStateChanged):
			return true
		case errors.Is(err, store.ErrPreconditionFailed):
			h.redirectToEventPage(w, r, calendarID, map[string]string{"error": eventChangedElsewhereMessage})
			return false
		case errors.Is(err, store.ErrLockConflict):
			h.redirectToEventPage(w, r, calendarID, map[string]string{"error": eventLockedMessage})
			return false
		default:
			if writeStoreUnavailable(w, err) {
				return false
			}
			httperrors.LogError(r, "failed to delete occurrence", err)
			h.redirectToEventPage(w, r, calendarID, map[string]string{"error": "failed to delete occurrence"})
			return false
		}
		h.redirectToEventPage(w, r, calendarID, map[string]string{"status": "occurrence_deleted"})
		return false
	}

	existing, err := h.store.Events.GetByUID(r.Context(), calendarID, uid)
	if err != nil {
		writeServerFailure(w, err, "failed to load event")
		return false
	}
	resourceName := ""
	if existing != nil {
		resourceName = calendarEventResourceName(uid, existing)
	}
	guard, err := h.pinEventACL(r.Context(), user, eventACLScope{calendarID: calendarID, calendar: &cal.Calendar, resourceName: resourceName})
	if err != nil {
		writeServerFailure(w, err, "failed to evaluate event access")
		return false
	}
	if existing, err = h.events.AuthorizeEventRemoval(r.Context(), user, calendarID, existing); err != nil {
		writeEventAccessError(w, err)
		return false
	}
	if existing == nil {
		http.Error(w, "event not found", http.StatusNotFound)
		return false
	}
	if !h.requireOpenedETag(w, r, calendarID, existing) {
		return false
	}
	resourcePath := calendarEventResourcePath(calendarID, calendarEventResourceName(uid, existing))
	return h.deleteEventState(w, r, calendarID, expectedEventState(existing, guard), resourcePath)
}

// deleteEventState removes the event expected describes, with the DAV state
// stored at resourcePath, and answers the request. It reports true, having
// answered nothing, when the event or the ACL it pins has changed since it was
// read.
func (h *Handler) deleteEventState(w http.ResponseWriter, r *http.Request, calendarID int64, expected store.DAVResourceState, resourcePath string) (changedConcurrently bool) {
	err := h.store.DeleteEventAndState(r.Context(), calendarID, expected, resourcePath, memberLockPreconditions(calendarID, expected.ResourceName))
	switch {
	case err == nil:
		h.redirectToEventPage(w, r, calendarID, map[string]string{"status": "event_deleted"})
		return false
	case errors.Is(err, store.ErrResourceStateChanged):
		return true
	case errors.Is(err, store.ErrLockConflict):
		h.redirectToEventPage(w, r, calendarID, map[string]string{"error": eventLockedMessage})
		return false
	default:
		if writeStoreUnavailable(w, err) {
			return false
		}
		httperrors.LogError(r, "failed to delete event", err)
		h.redirectToEventPage(w, r, calendarID, map[string]string{"error": "failed to delete event"})
		return false
	}
}

// authorizedEventWrite loads the event uid of cal for a write needing
// DAV:write-content on it, as REST and DAV decide that write, and answers the
// request itself when the write is refused. A user who may not read the event
// is answered as for a UID the calendar does not hold. It pins the ACL entries
// that decide the write, those of cal and of the event and those of any scope
// in also, in the guard it returns for the write to carry.
func (h *Handler) authorizedEventWrite(w http.ResponseWriter, r *http.Request, user *store.User, cal *store.CalendarAccess, uid string, also ...eventACLScope) (*store.Event, *store.ACLGuard, bool) {
	existing, err := h.store.Events.GetByUID(r.Context(), cal.ID, uid)
	if err != nil {
		writeServerFailure(w, err, "failed to load event")
		return nil, nil, false
	}
	resourceName := ""
	if existing != nil {
		resourceName = calendarEventResourceName(uid, existing)
	}
	guard, err := h.pinEventACL(r.Context(), user, append([]eventACLScope{{calendarID: cal.ID, calendar: &cal.Calendar, resourceName: resourceName}}, also...)...)
	if err != nil {
		writeServerFailure(w, err, "failed to evaluate event access")
		return nil, nil, false
	}
	if _, existing, err = h.events.AuthorizeEventWrite(r.Context(), user, cal.ID, existing, "write-content"); err != nil {
		writeEventAccessError(w, err)
		return nil, nil, false
	}
	if existing == nil {
		http.Error(w, "event not found", http.StatusNotFound)
		return nil, nil, false
	}
	return existing, guard, true
}

// unwritableEventMessage is the flash text for a write refused because the
// event is stored under a name no href carries, as a 1.1.x release could store
// one: "." or "..", which name the calendar or its parent once a path is
// cleaned.
const unwritableEventMessage = "This event is stored under a name that cannot be kept. Delete it, or move it to another name in a CalDAV client."

// writeEventAccessError answers a write to or removal of an event that the
// events service refused. A refusal that conceals the event is answered
// exactly as a UID the calendar does not hold is.
func writeEventAccessError(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, events.ErrNotFound):
		http.Error(w, "event not found", http.StatusNotFound)
	case errors.Is(err, events.ErrForbidden):
		http.Error(w, "forbidden", http.StatusForbidden)
	default:
		writeServerFailure(w, err, "failed to load event")
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
// nil. Reading the object's zones draws on work, the budget of the request,
// when it is not nil. An event read after the budget ran out is placed as UTC
// where its zone could not be followed, and its payload says so with
// "zoneComplete": false.
func unfoldedEventJSON(unfolded unfoldedEvent, viewStart, viewEnd time.Time, onsets ical.ObservanceOnsets, work *ical.RequestWork) map[string]any {
	ev, lines := unfolded.event, unfolded.lines
	payload := map[string]any{
		"uid":     ev.UID,
		"etag":    ev.ETag,
		"ical":    ev.RawICAL,
		"lastMod": ev.LastModified.Format(time.RFC3339),
	}

	zones := calendarEventZones{times: ical.NewObjectTimes(lines, onsets, work)}
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
	if zones.times.Starved() {
		// The request's work ran out before the object's zone could be followed,
		// so its times were read as UTC. The page says so beside them rather than
		// showing them as right, as it does for a series with instancesComplete.
		payload["zoneComplete"] = false
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
// host's database, which the browser shares, is left out. A name neither the
// object nor the host defines, which the DAV layer reads in UTC, is listed at
// a UTC offset, since the browser's own database may know the name in another
// case.
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
		if !zone.Defined() {
			// The server reads a TZID neither the object nor the host
			// defines in UTC; the browser's Intl, which accepts a name in
			// any case, must not read it in a zone of its own.
			listings[tzid] = []calendarTimezoneOffset{{From: merged[0].from.UTC().Format(time.RFC3339), Offset: 0}}
			continue
		}
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
	Floating           bool
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
	RecurrenceFloating bool
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
	payload["floating"] = m.Floating
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
		payload["recurrenceFloating"] = m.RecurrenceFloating
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
				event.RecurrenceFloating = floatingTime(params, value, allDay)
				if event.RecurrenceTimezone == "" && !allDay && !event.RecurrenceFloating {
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
				event.Floating = floatingTime(params, value, allDay)
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
		params[strings.ToUpper(strings.TrimSpace(name))] = strings.TrimSpace(strings.Trim(strings.TrimSpace(val), `"`))
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
