package ui

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"gitea.jw6.us/james/calcard/internal/auth"
	"gitea.jw6.us/james/calcard/internal/config"
	"gitea.jw6.us/james/calcard/internal/http/csrf"
	"gitea.jw6.us/james/calcard/internal/store"
	"github.com/go-chi/chi/v5"
)

const defaultPageSize = 50

// Handler serves server-rendered HTML pages.
type Handler struct {
	cfg         *config.Config
	store       *store.Store
	authService *auth.Service
	templates   map[string]*template.Template
}

func NewHandler(cfg *config.Config, store *store.Store, authService *auth.Service) *Handler {
	return &Handler{cfg: cfg, store: store, authService: authService, templates: templates}
}

func (h *Handler) Dashboard(w http.ResponseWriter, r *http.Request) {
	user, _ := auth.UserFromContext(r.Context())
	calendars, err := h.store.Calendars.ListAccessible(r.Context(), user.ID)
	if err != nil {
		http.Error(w, "failed to load calendars", http.StatusInternalServerError)
		return
	}
	books, err := h.store.AddressBooks.ListByUser(r.Context(), user.ID)
	if err != nil {
		http.Error(w, "failed to load address books", http.StatusInternalServerError)
		return
	}
	passwords, err := h.store.AppPasswords.ListByUser(r.Context(), user.ID)
	if err != nil {
		http.Error(w, "failed to load app passwords", http.StatusInternalServerError)
		return
	}

	// Fetch recent events using the optimized query
	recentEvents, err := h.store.Events.ListRecentByUser(r.Context(), user.ID, 5)
	if err != nil {
		http.Error(w, "failed to load recent events", http.StatusInternalServerError)
		return
	}

	// Build calendar name lookup
	calendarNames := make(map[int64]string)
	for _, cal := range calendars {
		calendarNames[cal.ID] = cal.Name
	}

	// Build event view data using parsed fields
	var eventData []map[string]any
	for _, ev := range recentEvents {
		summary := "Untitled Event"
		if ev.Summary != nil {
			summary = *ev.Summary
		}
		eventData = append(eventData, map[string]any{
			"CalendarID":   ev.CalendarID,
			"CalendarName": calendarNames[ev.CalendarID],
			"UID":          ev.UID,
			"Summary":      summary,
			"DTStart":      ev.DTStart,
			"AllDay":       ev.AllDay,
			"LastModified": ev.LastModified,
		})
	}

	// Fetch recent contacts using the optimized query
	recentContacts, err := h.store.Contacts.ListRecentByUser(r.Context(), user.ID, 5)
	if err != nil {
		http.Error(w, "failed to load recent contacts", http.StatusInternalServerError)
		return
	}

	// Build address book name lookup
	bookNames := make(map[int64]string)
	for _, book := range books {
		bookNames[book.ID] = book.Name
	}

	// Build contact view data using parsed fields
	var contactData []map[string]any
	for _, c := range recentContacts {
		displayName := "Unnamed Contact"
		if c.DisplayName != nil {
			displayName = *c.DisplayName
		}
		var email string
		if c.PrimaryEmail != nil {
			email = *c.PrimaryEmail
		}
		contactData = append(contactData, map[string]any{
			"AddressBookID":   c.AddressBookID,
			"AddressBookName": bookNames[c.AddressBookID],
			"UID":             c.UID,
			"DisplayName":     displayName,
			"Email":           email,
			"LastModified":    c.LastModified,
		})
	}

	data := h.withFlash(r, map[string]any{
		"Title":          "Dashboard",
		"User":           user,
		"CalendarCount":  len(calendars),
		"BookCount":      len(books),
		"AppPwdCount":    len(passwords),
		"RecentEvents":   eventData,
		"RecentContacts": contactData,
	})

	h.render(w, "dashboard.html", data)
}

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

func (h *Handler) AddressBooks(w http.ResponseWriter, r *http.Request) {
	user, _ := auth.UserFromContext(r.Context())
	books, err := h.store.AddressBooks.ListByUser(r.Context(), user.ID)
	if err != nil {
		http.Error(w, "failed to load address books", http.StatusInternalServerError)
		return
	}
	data := h.withFlash(r, map[string]any{
		"Title": "Address Books",
		"User":  user,
		"Books": books,
	})
	h.render(w, "addressbooks.html", data)
}

func (h *Handler) AppPasswords(w http.ResponseWriter, r *http.Request) {
	user, _ := auth.UserFromContext(r.Context())

	switch r.Method {
	case http.MethodPost:
		h.createAppPassword(w, r, user)
	default:
		h.renderAppPasswords(w, r, user, "")
	}
}

func (h *Handler) createAppPassword(w http.ResponseWriter, r *http.Request, user *store.User) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}
	label := r.FormValue("label")
	if label == "" {
		http.Error(w, "label is required", http.StatusBadRequest)
		return
	}
	expiresAtStr := r.FormValue("expires_at")

	var expiresAt *time.Time
	if expiresAtStr != "" {
		t, err := time.Parse(time.RFC3339, expiresAtStr)
		if err != nil {
			http.Error(w, "invalid expires_at format", http.StatusBadRequest)
			return
		}
		expiresAt = &t
	}

	plaintext, _, err := h.authService.CreateAppPassword(r.Context(), user.ID, label, expiresAt)
	if err != nil {
		http.Error(w, "failed to create app password", http.StatusInternalServerError)
		return
	}

	h.renderAppPasswords(w, r, user, plaintext)
}

func (h *Handler) RevokeAppPassword(w http.ResponseWriter, r *http.Request) {
	user, _ := auth.UserFromContext(r.Context())

	idStr := chi.URLParam(r, "id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil {
		http.Error(w, "invalid token id", http.StatusBadRequest)
		return
	}

	token, err := h.store.AppPasswords.GetByID(r.Context(), id)
	if err != nil {
		http.Error(w, "failed to load app password", http.StatusInternalServerError)
		return
	}
	if token == nil || token.UserID != user.ID {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if token.RevokedAt == nil {
		if err := h.store.AppPasswords.Revoke(r.Context(), id); err != nil {
			http.Error(w, "failed to revoke app password", http.StatusInternalServerError)
			return
		}
	}

	http.Redirect(w, r, "/app-passwords", http.StatusFound)
}

func (h *Handler) renderAppPasswords(w http.ResponseWriter, r *http.Request, user *store.User, plaintext string) {
	passwords, err := h.store.AppPasswords.ListByUser(r.Context(), user.ID)
	if err != nil {
		http.Error(w, "failed to load app passwords", http.StatusInternalServerError)
		return
	}

	var view []map[string]any
	now := time.Now()
	for _, p := range passwords {
		status := "active"
		revoked := p.RevokedAt != nil
		expired := p.ExpiresAt != nil && p.ExpiresAt.Before(now)
		if revoked {
			status = "revoked"
		} else if expired {
			status = "expired"
		}
		view = append(view, map[string]any{
			"id":         p.ID,
			"label":      p.Label,
			"created_at": p.CreatedAt,
			"expires_at": p.ExpiresAt,
			"last_used":  p.LastUsedAt,
			"status":     status,
			"revoked":    revoked,
			"expired":    expired,
		})
	}
	data := h.withFlash(r, map[string]any{
		"Title":        "App Passwords",
		"User":         user,
		"AppPasswords": view,
	})
	if plaintext != "" {
		data["PlainToken"] = plaintext
		data["FlashMessage"] = "created"
	}
	h.render(w, "app_passwords.html", data)
}

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

func (h *Handler) CreateAddressBook(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		h.redirect(w, r, "/addressbooks", map[string]string{"error": "invalid form"})
		return
	}
	name := strings.TrimSpace(r.FormValue("name"))
	if name == "" {
		h.redirect(w, r, "/addressbooks", map[string]string{"error": "name is required"})
		return
	}
	user, _ := auth.UserFromContext(r.Context())
	_, err := h.store.AddressBooks.Create(r.Context(), store.AddressBook{UserID: user.ID, Name: name})
	if err != nil {
		h.redirect(w, r, "/addressbooks", map[string]string{"error": "failed to create"})
		return
	}
	h.redirect(w, r, "/addressbooks", map[string]string{"status": "created"})
}

func (h *Handler) RenameAddressBook(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		h.redirect(w, r, "/addressbooks", map[string]string{"error": "invalid form"})
		return
	}
	name := strings.TrimSpace(r.FormValue("name"))
	if name == "" {
		h.redirect(w, r, "/addressbooks", map[string]string{"error": "name is required"})
		return
	}
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		h.redirect(w, r, "/addressbooks", map[string]string{"error": "invalid id"})
		return
	}
	user, _ := auth.UserFromContext(r.Context())
	book, err := h.store.AddressBooks.GetByID(r.Context(), id)
	if err != nil {
		h.redirect(w, r, "/addressbooks", map[string]string{"error": "rename failed"})
		return
	}
	if book == nil || book.UserID != user.ID {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
	if err := h.store.AddressBooks.Rename(r.Context(), user.ID, id, name); err != nil {
		h.redirect(w, r, "/addressbooks", map[string]string{"error": "rename failed"})
		return
	}
	h.redirect(w, r, "/addressbooks", map[string]string{"status": "renamed"})
}

func (h *Handler) DeleteAddressBook(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		h.redirect(w, r, "/addressbooks", map[string]string{"error": "invalid id"})
		return
	}
	user, _ := auth.UserFromContext(r.Context())
	if err := h.store.AddressBooks.Delete(r.Context(), user.ID, id); err != nil {
		h.redirect(w, r, "/addressbooks", map[string]string{"error": "delete failed"})
		return
	}
	h.redirect(w, r, "/addressbooks", map[string]string{"status": "deleted"})
}

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

	// Parse pagination params
	page, limit := h.parsePagination(r)
	offset := (page - 1) * limit

	result, err := h.store.Events.ListForCalendarPaginated(r.Context(), id, limit, offset)
	if err != nil {
		http.Error(w, "failed to load events", http.StatusInternalServerError)
		return
	}

	// Build view data with parsed fields
	var eventData []map[string]any
	var eventsJSONData []map[string]any
	for _, ev := range result.Items {
		summary := "Untitled Event"
		if ev.Summary != nil {
			summary = *ev.Summary
		}
		eventData = append(eventData, map[string]any{
			"UID":          ev.UID,
			"Summary":      summary,
			"DTStart":      ev.DTStart,
			"DTEnd":        ev.DTEnd,
			"AllDay":       ev.AllDay,
			"LastModified": ev.LastModified,
			"RawICAL":      ev.RawICAL,
		})

		eventsJSONData = append(eventsJSONData, map[string]any{
			"uid":     ev.UID,
			"ical":    ev.RawICAL,
			"lastMod": ev.LastModified.Format(time.RFC3339),
		})
	}
	eventsJSON, err := json.Marshal(eventsJSONData)
	if err != nil {
		http.Error(w, "failed to render events", http.StatusInternalServerError)
		return
	}

	totalPages := (result.TotalCount + limit - 1) / limit
	data := h.withFlash(r, map[string]any{
		"Title":      cal.Name + " - Calendar",
		"User":       user,
		"Calendar":   cal,
		"Events":     eventData,
		"EventsJSON": template.JS(eventsJSON),
		"Page":       page,
		"Limit":      limit,
		"TotalCount": result.TotalCount,
		"TotalPages": totalPages,
		"HasPrev":    page > 1,
		"HasNext":    page < totalPages,
		"PrevPage":   page - 1,
		"NextPage":   page + 1,
	})
	h.render(w, "calendar_view.html", data)
}

func (h *Handler) ViewAddressBook(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		http.Error(w, "invalid address book id", http.StatusBadRequest)
		return
	}
	user, _ := auth.UserFromContext(r.Context())
	book, err := h.store.AddressBooks.GetByID(r.Context(), id)
	if err != nil {
		http.Error(w, "failed to load address book", http.StatusInternalServerError)
		return
	}
	if book == nil || book.UserID != user.ID {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	// Parse pagination params
	page, limit := h.parsePagination(r)
	offset := (page - 1) * limit

	result, err := h.store.Contacts.ListForBookPaginated(r.Context(), id, limit, offset)
	if err != nil {
		http.Error(w, "failed to load contacts", http.StatusInternalServerError)
		return
	}

	// Build view data with parsed fields
	var contactData []map[string]any
	for _, c := range result.Items {
		displayName := "Unnamed Contact"
		if c.DisplayName != nil {
			displayName = *c.DisplayName
		}
		var email string
		if c.PrimaryEmail != nil {
			email = *c.PrimaryEmail
		}
		contactData = append(contactData, map[string]any{
			"UID":          c.UID,
			"DisplayName":  displayName,
			"Email":        email,
			"LastModified": c.LastModified,
			"RawVCard":     c.RawVCard,
		})
	}

	totalPages := (result.TotalCount + limit - 1) / limit
	data := h.withFlash(r, map[string]any{
		"Title":       book.Name + " - Address Book",
		"User":        user,
		"AddressBook": book,
		"Contacts":    contactData,
		"Page":        page,
		"Limit":       limit,
		"TotalCount":  result.TotalCount,
		"TotalPages":  totalPages,
		"HasPrev":     page > 1,
		"HasNext":     page < totalPages,
		"PrevPage":    page - 1,
		"NextPage":    page + 1,
	})
	h.render(w, "addressbook_view.html", data)
}

func (h *Handler) Sessions(w http.ResponseWriter, r *http.Request) {
	user, _ := auth.UserFromContext(r.Context())

	sessions, err := h.store.Sessions.ListByUser(r.Context(), user.ID)
	if err != nil {
		http.Error(w, "failed to load sessions", http.StatusInternalServerError)
		return
	}

	// Get current session ID from context to highlight it
	currentSessionID := auth.SessionIDFromContext(r.Context())

	var sessionData []map[string]any
	for _, s := range sessions {
		userAgent := ""
		if s.UserAgent != nil {
			userAgent = *s.UserAgent
		}
		ipAddress := ""
		if s.IPAddress != nil {
			ipAddress = *s.IPAddress
		}
		sessionData = append(sessionData, map[string]any{
			"ID":         s.ID,
			"UserAgent":  userAgent,
			"IPAddress":  ipAddress,
			"CreatedAt":  s.CreatedAt,
			"ExpiresAt":  s.ExpiresAt,
			"LastSeenAt": s.LastSeenAt,
			"IsCurrent":  s.ID == currentSessionID,
		})
	}

	data := h.withFlash(r, map[string]any{
		"Title":    "Active Sessions",
		"User":     user,
		"Sessions": sessionData,
	})
	h.render(w, "sessions.html", data)
}

func (h *Handler) RevokeSession(w http.ResponseWriter, r *http.Request) {
	user, _ := auth.UserFromContext(r.Context())
	sessionID := chi.URLParam(r, "id")

	// Verify session belongs to user
	session, err := h.store.Sessions.GetByID(r.Context(), sessionID)
	if err != nil {
		http.Error(w, "failed to load session", http.StatusInternalServerError)
		return
	}
	if session == nil || session.UserID != user.ID {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	if err := h.store.Sessions.Delete(r.Context(), sessionID); err != nil {
		http.Error(w, "failed to revoke session", http.StatusInternalServerError)
		return
	}

	h.redirect(w, r, "/sessions", map[string]string{"status": "revoked"})
}

func (h *Handler) RevokeAllSessions(w http.ResponseWriter, r *http.Request) {
	user, _ := auth.UserFromContext(r.Context())
	currentSessionID := auth.SessionIDFromContext(r.Context())

	// Get all sessions for user
	sessions, err := h.store.Sessions.ListByUser(r.Context(), user.ID)
	if err != nil {
		http.Error(w, "failed to load sessions", http.StatusInternalServerError)
		return
	}

	// Delete all except current
	for _, s := range sessions {
		if s.ID != currentSessionID {
			_ = h.store.Sessions.Delete(r.Context(), s.ID)
		}
	}

	h.redirect(w, r, "/sessions", map[string]string{"status": "all_revoked"})
}

func (h *Handler) CreateAppPassword(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}
	label := strings.TrimSpace(r.FormValue("label"))
	if label == "" {
		http.Error(w, "label is required", http.StatusBadRequest)
		return
	}
	var expiresAt *time.Time
	if exp := strings.TrimSpace(r.FormValue("expires_at")); exp != "" {
		// Try datetime-local format first (from HTML5 input), then RFC3339
		var parsed time.Time
		var err error
		parsed, err = time.ParseInLocation("2006-01-02T15:04", exp, time.Local)
		if err != nil {
			parsed, err = time.Parse(time.RFC3339, exp)
		}
		if err != nil {
			http.Error(w, "invalid expiry format", http.StatusBadRequest)
			return
		}
		expiresAt = &parsed
	}

	user, _ := auth.UserFromContext(r.Context())
	token, _, err := h.authService.CreateAppPassword(r.Context(), user.ID, label, expiresAt)
	if err != nil {
		http.Error(w, "create failed", http.StatusInternalServerError)
		return
	}

	h.renderAppPasswords(w, r, user, token)
}

func (h *Handler) Logout(w http.ResponseWriter, r *http.Request) {
	h.authService.RequireSession(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		h.authService.ClearSession(w, r)
		http.Redirect(w, r, "/auth/login", http.StatusFound)
	})).ServeHTTP(w, r)
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
	recurrence := parseRecurrenceOptions(r)

	uid := generateUID()
	ical := buildICalEvent(uid, summary, dtstart, dtend, allDay, location, description, recurrence)
	etag := generateETag(ical)

	if _, err := h.store.Events.Upsert(r.Context(), store.Event{
		CalendarID: calendarID,
		UID:        uid,
		RawICAL:    ical,
		ETag:       etag,
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
	recurrence := parseRecurrenceOptions(r)

	ical := ""
	if editScope == "occurrence" && recurrenceID != "" && existing.RawICAL != "" {
		// Update only a single occurrence by replacing/adding a RECURRENCE-ID component.
		header, components, footer := splitICalComponents(existing.RawICAL)
		override := buildICalEventComponent(uid, summary, dtstart, dtend, allDay, location, description, nil, "")
		if recLine, err := formatICalDateTime(recurrenceID, recurrenceAllDay, false, "RECURRENCE-ID"); err == nil && recLine != "" {
			if len(override) >= 2 {
				override = append(override[:2], append([]string{recLine}, override[2:]...)...)
			} else {
				override = append([]string{recLine}, override...)
			}
		}
		targetRecID := recurrenceIDValue(override)

		var newComponents [][]string
		for _, comp := range components {
			if recurrenceIDValue(comp) == targetRecID {
				continue
			}
			newComponents = append(newComponents, comp)
		}
		newComponents = append(newComponents, override)
		ical = buildICalFromComponents(header, newComponents, footer)
	} else {
		// Update the series/master event while keeping overrides intact.
		header, components, footer := splitICalComponents(existing.RawICAL)
		master := buildICalEventComponent(uid, summary, dtstart, dtend, allDay, location, description, recurrence, "")
		replaced := false
		for i, comp := range components {
			if recurrenceIDValue(comp) == "" && !replaced {
				components[i] = master
				replaced = true
			}
		}
		if !replaced {
			components = append([][]string{master}, components...)
		}
		ical = buildICalFromComponents(header, components, footer)
	}
	etag := generateETag(ical)

	if _, err := h.store.Events.Upsert(r.Context(), store.Event{
		CalendarID: calendarID,
		UID:        uid,
		RawICAL:    ical,
		ETag:       etag,
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

		exdateLine, err := formatICalDateTime(recurrenceID, recurrenceAllDay, false, "EXDATE")
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

		header, components, footer := splitICalComponents(existing.RawICAL)
		var newComponents [][]string
		masterHandled := false
		for _, comp := range components {
			recID := recurrenceIDValue(comp)
			if recID == targetValue {
				// Drop an overridden occurrence matching the target.
				continue
			}
			if recID == "" && !masterHandled {
				if !hasPropertyValue(comp, "EXDATE", targetValue) {
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

		updatedICAL := buildICalFromComponents(header, newComponents, footer)
		if _, err := h.store.Events.Upsert(r.Context(), store.Event{
			CalendarID: calendarID,
			UID:        uid,
			RawICAL:    updatedICAL,
			ETag:       generateETag(updatedICAL),
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

// CreateContact creates a new contact in an address book.
func (h *Handler) CreateContact(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}

	bookID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		http.Error(w, "invalid address book id", http.StatusBadRequest)
		return
	}

	user, _ := auth.UserFromContext(r.Context())
	book, err := h.store.AddressBooks.GetByID(r.Context(), bookID)
	if err != nil {
		http.Error(w, "failed to load address book", http.StatusInternalServerError)
		return
	}
	if book == nil || book.UserID != user.ID {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	displayName := strings.TrimSpace(r.FormValue("display_name"))
	if displayName == "" {
		h.redirect(w, r, fmt.Sprintf("/addressbooks/%d", bookID), map[string]string{"error": "name is required"})
		return
	}

	firstName := strings.TrimSpace(r.FormValue("first_name"))
	lastName := strings.TrimSpace(r.FormValue("last_name"))
	email := strings.TrimSpace(r.FormValue("email"))
	phone := strings.TrimSpace(r.FormValue("phone"))
	birthday := strings.TrimSpace(r.FormValue("birthday"))
	notes := strings.TrimSpace(r.FormValue("notes"))

	uid := generateUID()
	vcard := buildVCard(uid, displayName, firstName, lastName, email, phone, birthday, notes)
	etag := generateETag(vcard)

	if _, err := h.store.Contacts.Upsert(r.Context(), store.Contact{
		AddressBookID: bookID,
		UID:           uid,
		RawVCard:      vcard,
		ETag:          etag,
	}); err != nil {
		h.redirect(w, r, fmt.Sprintf("/addressbooks/%d", bookID), map[string]string{"error": "failed to create contact"})
		return
	}

	h.redirect(w, r, fmt.Sprintf("/addressbooks/%d", bookID), map[string]string{"status": "contact_created"})
}

// UpdateContact updates an existing contact.
func (h *Handler) UpdateContact(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}

	bookID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		http.Error(w, "invalid address book id", http.StatusBadRequest)
		return
	}

	uid := chi.URLParam(r, "uid")
	if uid == "" {
		http.Error(w, "invalid contact uid", http.StatusBadRequest)
		return
	}

	user, _ := auth.UserFromContext(r.Context())
	book, err := h.store.AddressBooks.GetByID(r.Context(), bookID)
	if err != nil {
		http.Error(w, "failed to load address book", http.StatusInternalServerError)
		return
	}
	if book == nil || book.UserID != user.ID {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	existing, err := h.store.Contacts.GetByUID(r.Context(), bookID, uid)
	if err != nil {
		http.Error(w, "failed to load contact", http.StatusInternalServerError)
		return
	}
	if existing == nil {
		http.Error(w, "contact not found", http.StatusNotFound)
		return
	}

	displayName := strings.TrimSpace(r.FormValue("display_name"))
	if displayName == "" {
		h.redirect(w, r, fmt.Sprintf("/addressbooks/%d", bookID), map[string]string{"error": "name is required"})
		return
	}

	firstName := strings.TrimSpace(r.FormValue("first_name"))
	lastName := strings.TrimSpace(r.FormValue("last_name"))
	email := strings.TrimSpace(r.FormValue("email"))
	phone := strings.TrimSpace(r.FormValue("phone"))
	birthday := strings.TrimSpace(r.FormValue("birthday"))
	notes := strings.TrimSpace(r.FormValue("notes"))

	vcard := buildVCard(uid, displayName, firstName, lastName, email, phone, birthday, notes)
	etag := generateETag(vcard)

	if _, err := h.store.Contacts.Upsert(r.Context(), store.Contact{
		AddressBookID: bookID,
		UID:           uid,
		RawVCard:      vcard,
		ETag:          etag,
	}); err != nil {
		h.redirect(w, r, fmt.Sprintf("/addressbooks/%d", bookID), map[string]string{"error": "failed to update contact"})
		return
	}

	h.redirect(w, r, fmt.Sprintf("/addressbooks/%d", bookID), map[string]string{"status": "contact_updated"})
}

// DeleteContact removes a contact from an address book.
func (h *Handler) DeleteContact(w http.ResponseWriter, r *http.Request) {
	bookID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		http.Error(w, "invalid address book id", http.StatusBadRequest)
		return
	}

	uid := chi.URLParam(r, "uid")
	if uid == "" {
		http.Error(w, "invalid contact uid", http.StatusBadRequest)
		return
	}

	user, _ := auth.UserFromContext(r.Context())
	book, err := h.store.AddressBooks.GetByID(r.Context(), bookID)
	if err != nil {
		http.Error(w, "failed to load address book", http.StatusInternalServerError)
		return
	}
	if book == nil || book.UserID != user.ID {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}

	if err := h.store.Contacts.DeleteByUID(r.Context(), bookID, uid); err != nil {
		h.redirect(w, r, fmt.Sprintf("/addressbooks/%d", bookID), map[string]string{"error": "failed to delete contact"})
		return
	}

	h.redirect(w, r, fmt.Sprintf("/addressbooks/%d", bookID), map[string]string{"status": "contact_deleted"})
}

// ViewBirthdays shows the virtual birthdays calendar.
func (h *Handler) ViewBirthdays(w http.ResponseWriter, r *http.Request) {
	user, _ := auth.UserFromContext(r.Context())

	contacts, err := h.store.Contacts.ListWithBirthdaysByUser(r.Context(), user.ID)
	if err != nil {
		http.Error(w, "failed to load contacts", http.StatusInternalServerError)
		return
	}

	// Generate birthday events
	var birthdayEvents []map[string]any
	currentYear := time.Now().Year()

	for _, c := range contacts {
		if c.Birthday == nil {
			continue
		}

		displayName := "Unknown"
		if c.DisplayName != nil {
			displayName = *c.DisplayName
		}

		// Create birthday event for current year
		bdayThisYear := time.Date(currentYear, c.Birthday.Month(), c.Birthday.Day(), 0, 0, 0, 0, time.UTC)

		// Calculate age if birth year is known (year > 1900, since older years are likely placeholders)
		var age *int
		if c.Birthday.Year() > 1900 {
			a := currentYear - c.Birthday.Year()
			age = &a
		}

		birthdayEvents = append(birthdayEvents, map[string]any{
			"ContactUID":  c.UID,
			"DisplayName": displayName,
			"Date":        bdayThisYear,
			"Age":         age,
			"Month":       int(c.Birthday.Month()),
			"Day":         c.Birthday.Day(),
		})
	}

	data := h.withFlash(r, map[string]any{
		"Title":          "Birthdays",
		"User":           user,
		"BirthdayEvents": birthdayEvents,
	})
	h.render(w, "birthdays.html", data)
}

// generateUID creates a unique identifier for calendar/contact objects.
func generateUID() string {
	return fmt.Sprintf("%d-%s@calcard", time.Now().UnixNano(), randomString(8))
}

// randomString generates a random alphanumeric string.
func randomString(n int) string {
	const letters = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = letters[time.Now().UnixNano()%int64(len(letters))]
		time.Sleep(time.Nanosecond)
	}
	return string(b)
}

// generateETag creates an ETag from content.
func generateETag(content string) string {
	h := sha256.Sum256([]byte(content))
	return fmt.Sprintf("%x", h)
}

// RecurrenceOptions holds recurrence rule parameters.
type RecurrenceOptions struct {
	Frequency string // DAILY, WEEKLY, MONTHLY, YEARLY
	Interval  int    // Every N days/weeks/months/years
	Count     int    // Number of occurrences (0 = no limit)
	Until     string // End date in YYYY-MM-DD format
}

// parseRecurrenceOptions extracts recurrence options from form data.
func parseRecurrenceOptions(r *http.Request) *RecurrenceOptions {
	freq := strings.TrimSpace(r.FormValue("recurrence"))
	if freq == "" {
		return nil
	}

	interval := 1
	if i, err := strconv.Atoi(r.FormValue("recurrence_interval")); err == nil && i > 0 {
		interval = i
	}

	var count int
	var until string
	endType := r.FormValue("recurrence_end_type")
	switch endType {
	case "after":
		if c, err := strconv.Atoi(r.FormValue("recurrence_count")); err == nil && c > 0 {
			count = c
		}
	case "on":
		until = strings.TrimSpace(r.FormValue("recurrence_until"))
	}

	return &RecurrenceOptions{
		Frequency: freq,
		Interval:  interval,
		Count:     count,
		Until:     until,
	}
}

// formatICalDateTime converts form inputs into iCalendar date/time strings.
func formatICalDateTime(value string, allDay bool, exclusiveEnd bool, prop string) (string, error) {
	if value == "" {
		return "", nil
	}

	if allDay {
		t, err := time.Parse("2006-01-02", value)
		if err != nil {
			return "", err
		}
		if exclusiveEnd {
			t = t.AddDate(0, 0, 1)
		}
		return fmt.Sprintf("%s;VALUE=DATE:%s", prop, t.Format("20060102")), nil
	}

	t, err := time.ParseInLocation("2006-01-02T15:04", value, time.Local)
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%s", prop, t.UTC().Format("20060102T150405Z")), nil
}

// buildICalEventComponent builds a single VEVENT component (without VCALENDAR wrapper).
func buildICalEventComponent(uid, summary, dtstart, dtend string, allDay bool, location, description string, recurrence *RecurrenceOptions, recurrenceID string) []string {
	var lines []string
	lines = append(lines, fmt.Sprintf("UID:%s", uid))
	lines = append(lines, fmt.Sprintf("DTSTAMP:%s", time.Now().UTC().Format("20060102T150405Z")))

	if recurrenceID != "" {
		if recLine, err := formatICalDateTime(recurrenceID, allDay, false, "RECURRENCE-ID"); err == nil && recLine != "" {
			lines = append(lines, recLine)
		}
	}

	if startLine, err := formatICalDateTime(dtstart, allDay, false, "DTSTART"); err == nil && startLine != "" {
		lines = append(lines, startLine)
	}
	if endLine, err := formatICalDateTime(dtend, allDay, true, "DTEND"); err == nil && endLine != "" {
		lines = append(lines, endLine)
	}

	lines = append(lines, fmt.Sprintf("SUMMARY:%s", escapeICalValue(summary)))

	if location != "" {
		lines = append(lines, fmt.Sprintf("LOCATION:%s", escapeICalValue(location)))
	}

	if description != "" {
		lines = append(lines, fmt.Sprintf("DESCRIPTION:%s", escapeICalValue(description)))
	}

	if recurrence != nil && recurrence.Frequency != "" && recurrenceID == "" {
		rrule := fmt.Sprintf("RRULE:FREQ=%s", recurrence.Frequency)
		if recurrence.Interval > 1 {
			rrule += fmt.Sprintf(";INTERVAL=%d", recurrence.Interval)
		}
		if recurrence.Count > 0 {
			rrule += fmt.Sprintf(";COUNT=%d", recurrence.Count)
		} else if recurrence.Until != "" {
			if t, err := time.Parse("2006-01-02", recurrence.Until); err == nil {
				rrule += fmt.Sprintf(";UNTIL=%s", t.Format("20060102"))
			}
		}
		lines = append(lines, rrule)
	}

	return lines
}

// buildICalEvent constructs a valid iCalendar event.
func buildICalEvent(uid, summary, dtstart, dtend string, allDay bool, location, description string, recurrence *RecurrenceOptions) string {
	eventLines := buildICalEventComponent(uid, summary, dtstart, dtend, allDay, location, description, recurrence, "")

	var sb strings.Builder
	sb.WriteString("BEGIN:VCALENDAR\r\n")
	sb.WriteString("VERSION:2.0\r\n")
	sb.WriteString("PRODID:-//CalCard//EN\r\n")
	sb.WriteString("BEGIN:VEVENT\r\n")
	for _, line := range eventLines {
		sb.WriteString(line + "\r\n")
	}
	sb.WriteString("END:VEVENT\r\n")
	sb.WriteString("END:VCALENDAR\r\n")

	return sb.String()
}

// unfoldICalLinesSimple unfolds folded lines without pulling in a full parser.
func unfoldICalLinesSimple(ical string) []string {
	ical = strings.ReplaceAll(ical, "\r\n", "\n")
	ical = strings.ReplaceAll(ical, "\r", "\n")
	rawLines := strings.Split(ical, "\n")
	var lines []string
	for _, line := range rawLines {
		if len(lines) > 0 && (strings.HasPrefix(line, " ") || strings.HasPrefix(line, "\t")) {
			lines[len(lines)-1] += strings.TrimLeft(line, " \t")
			continue
		}
		lines = append(lines, line)
	}
	return lines
}

// splitICalComponents separates VCALENDAR header/footer and VEVENT components.
func splitICalComponents(ical string) (header []string, events [][]string, footer []string) {
	lines := unfoldICalLinesSimple(ical)
	var current []string
	inEvent := false
	for _, line := range lines {
		switch line {
		case "BEGIN:VEVENT":
			inEvent = true
			current = nil
		case "END:VEVENT":
			if inEvent {
				events = append(events, current)
				current = nil
				inEvent = false
			}
		default:
			if inEvent {
				current = append(current, line)
			} else if len(events) == 0 {
				header = append(header, line)
			} else {
				footer = append(footer, line)
			}
		}
	}
	return
}

func buildICalFromComponents(header []string, events [][]string, footer []string) string {
	var sb strings.Builder
	write := func(line string) {
		if line == "" {
			return
		}
		if strings.HasSuffix(line, "\r\n") {
			sb.WriteString(line)
			return
		}
		sb.WriteString(line + "\r\n")
	}

	for _, line := range header {
		write(line)
	}
	for _, ev := range events {
		write("BEGIN:VEVENT")
		for _, line := range ev {
			write(line)
		}
		write("END:VEVENT")
	}
	for _, line := range footer {
		write(line)
	}
	return sb.String()
}

func recurrenceIDValue(lines []string) string {
	for _, line := range lines {
		if strings.HasPrefix(line, "RECURRENCE-ID") {
			parts := strings.SplitN(line, ":", 2)
			if len(parts) == 2 {
				return parts[1]
			}
		}
	}
	return ""
}

func hasPropertyValue(lines []string, prop, value string) bool {
	for _, line := range lines {
		if strings.HasPrefix(line, prop) {
			parts := strings.SplitN(line, ":", 2)
			if len(parts) == 2 && parts[1] == value {
				return true
			}
		}
	}
	return false
}

// escapeICalValue escapes special characters for iCalendar format.
func escapeICalValue(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, ";", "\\;")
	s = strings.ReplaceAll(s, ",", "\\,")
	s = strings.ReplaceAll(s, "\n", "\\n")
	return s
}

// buildVCard constructs a valid vCard 3.0.
func buildVCard(uid, displayName, firstName, lastName, email, phone, birthday, notes string) string {
	var sb strings.Builder
	sb.WriteString("BEGIN:VCARD\r\n")
	sb.WriteString("VERSION:3.0\r\n")
	sb.WriteString(fmt.Sprintf("UID:%s\r\n", uid))
	sb.WriteString(fmt.Sprintf("FN:%s\r\n", escapeVCardValue(displayName)))

	// N: Last;First;Middle;Prefix;Suffix
	sb.WriteString(fmt.Sprintf("N:%s;%s;;;\r\n", escapeVCardValue(lastName), escapeVCardValue(firstName)))

	if email != "" {
		sb.WriteString(fmt.Sprintf("EMAIL;TYPE=INTERNET:%s\r\n", email))
	}

	if phone != "" {
		sb.WriteString(fmt.Sprintf("TEL;TYPE=CELL:%s\r\n", phone))
	}

	if birthday != "" {
		// Handle both YYYY-MM-DD and --MM-DD formats
		if strings.HasPrefix(birthday, "--") {
			// No year specified, use --MM-DD vCard format
			sb.WriteString(fmt.Sprintf("BDAY:%s\r\n", birthday))
		} else if t, err := time.Parse("2006-01-02", birthday); err == nil {
			sb.WriteString(fmt.Sprintf("BDAY:%s\r\n", t.Format("2006-01-02")))
		}
	}

	if notes != "" {
		sb.WriteString(fmt.Sprintf("NOTE:%s\r\n", escapeVCardValue(notes)))
	}

	sb.WriteString(fmt.Sprintf("REV:%s\r\n", time.Now().UTC().Format("20060102T150405Z")))
	sb.WriteString("END:VCARD\r\n")

	return sb.String()
}

// escapeVCardValue escapes special characters for vCard format.
func escapeVCardValue(s string) string {
	s = strings.ReplaceAll(s, "\\", "\\\\")
	s = strings.ReplaceAll(s, ";", "\\;")
	s = strings.ReplaceAll(s, ",", "\\,")
	s = strings.ReplaceAll(s, "\n", "\\n")
	return s
}

func (h *Handler) parsePagination(r *http.Request) (page, limit int) {
	page = 1
	limit = defaultPageSize

	if p := r.URL.Query().Get("page"); p != "" {
		if parsed, err := strconv.Atoi(p); err == nil && parsed > 0 {
			page = parsed
		}
	}
	if l := r.URL.Query().Get("limit"); l != "" {
		if parsed, err := strconv.Atoi(l); err == nil && parsed > 0 && parsed <= 100 {
			limit = parsed
		}
	}
	return
}

func (h *Handler) withFlash(r *http.Request, data map[string]any) map[string]any {
	q := r.URL.Query()
	if status := q.Get("status"); status != "" {
		data["FlashMessage"] = status
	}
	if err := q.Get("error"); err != "" {
		data["FlashError"] = err
	}
	if token := q.Get("token"); token != "" {
		data["PlainToken"] = token
	}
	if csrfToken := csrf.TokenFromContext(r.Context()); csrfToken != "" {
		data["CSRFToken"] = csrfToken
	}
	return data
}

func (h *Handler) redirect(w http.ResponseWriter, r *http.Request, path string, params map[string]string) {
	q := url.Values{}
	for k, v := range params {
		if v != "" {
			q.Set(k, v)
		}
	}
	location := path
	if encoded := q.Encode(); encoded != "" {
		location += "?" + encoded
	}
	http.Redirect(w, r, location, http.StatusFound)
}

func (h *Handler) render(w http.ResponseWriter, name string, data any) {
	tmpl, ok := h.templates[name]
	if !ok {
		http.Error(w, fmt.Sprintf("template %q not found", name), http.StatusInternalServerError)
		return
	}

	if err := tmpl.ExecuteTemplate(w, name, data); err != nil {
		http.Error(w, fmt.Sprintf("template error: %v", err), http.StatusInternalServerError)
	}
}
