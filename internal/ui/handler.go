package ui

import (
	"html/template"
	"net/http"
	"time"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/store"
)

// Handler serves server-rendered HTML pages.
type Handler struct {
	cfg         *config.Config
	store       *store.Store
	authService *auth.Service
	templates   map[string]*template.Template
}

// NewHandler creates a new Handler instance.
func NewHandler(cfg *config.Config, store *store.Store, authService *auth.Service) *Handler {
	return &Handler{cfg: cfg, store: store, authService: authService, templates: templates}
}

// Dashboard displays the main dashboard.
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

	h.render(w, r, "dashboard.html", data)
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
	h.render(w, r, "birthdays.html", data)
}
