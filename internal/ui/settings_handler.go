package ui

import (
	"net/http"
	"strconv"
	"time"

	"github.com/jw6ventures/calcard/internal/auth"
)

// weekdayOption is one choice in the first-day-of-the-week list.
type weekdayOption struct {
	Value    int
	Name     string
	Selected bool
}

// Settings shows the signed-in user's preferences.
func (h *Handler) Settings(w http.ResponseWriter, r *http.Request) {
	user, _ := auth.UserFromContext(r.Context())

	options := make([]weekdayOption, 0, 7)
	for day := time.Sunday; day <= time.Saturday; day++ {
		options = append(options, weekdayOption{Value: int(day), Name: day.String(), Selected: day == user.WeekStart})
	}

	data := h.withFlash(r, map[string]any{
		"Title":          "Settings",
		"User":           user,
		"WeekdayOptions": options,
	})
	h.render(w, r, "settings.html", data)
}

// UpdateSettings saves the preferences submitted from the settings page.
func (h *Handler) UpdateSettings(w http.ResponseWriter, r *http.Request) {
	if err := r.ParseForm(); err != nil {
		http.Error(w, "invalid form", http.StatusBadRequest)
		return
	}
	weekStart, ok := parseWeekday(r.FormValue("week_start"))
	if !ok {
		http.Error(w, "first day of the week must be a weekday", http.StatusBadRequest)
		return
	}

	user, _ := auth.UserFromContext(r.Context())
	if err := h.store.Users.SetWeekStart(r.Context(), user.ID, weekStart); err != nil {
		writeServerFailure(w, err, "failed to save settings")
		return
	}

	h.redirect(w, r, "/settings", map[string]string{"status": "settings_saved"})
}

// parseWeekday reads a weekday submitted as its number, 0 (Sunday) to 6.
func parseWeekday(value string) (time.Weekday, bool) {
	day, err := strconv.Atoi(value)
	if err != nil || day < int(time.Sunday) || day > int(time.Saturday) {
		return 0, false
	}
	return time.Weekday(day), true
}
