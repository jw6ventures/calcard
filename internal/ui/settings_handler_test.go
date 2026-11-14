package ui

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/dav"
	"github.com/jw6ventures/calcard/internal/store"
)

func TestSettingsShowsSavedFirstDayOfWeek(t *testing.T) {
	user := &store.User{ID: 100, PrimaryEmail: "user@example.com", WeekStart: time.Thursday}
	handler := NewHandler(&config.Config{}, &store.Store{}, nil, dav.ValidateCalendarObject, dav.EventInstances)

	req := httptest.NewRequest(http.MethodGet, "/settings", nil)
	req = req.WithContext(auth.WithUser(req.Context(), user))
	w := httptest.NewRecorder()
	handler.Settings(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Settings() status = %d, want %d", w.Code, http.StatusOK)
	}
	body := w.Body.String()
	if !strings.Contains(body, "First Day of the Week") {
		t.Fatalf("settings page has no first day of the week field:\n%s", body)
	}
	for day := time.Sunday; day <= time.Saturday; day++ {
		option := `<option value="` + strconv.Itoa(int(day)) + `"`
		if day == time.Thursday {
			option += " selected"
		}
		option += ">" + day.String() + "</option>"
		if !strings.Contains(body, option) {
			t.Errorf("settings page is missing %s", option)
		}
	}
}

func TestUpdateSettingsSavesFirstDayOfWeek(t *testing.T) {
	for day := time.Sunday; day <= time.Saturday; day++ {
		t.Run(day.String(), func(t *testing.T) {
			users := &fakeUserRepo{users: map[int64]*store.User{100: {ID: 100, WeekStart: time.Tuesday}}}
			handler := NewHandler(&config.Config{}, &store.Store{Users: users}, nil, dav.ValidateCalendarObject, dav.EventInstances)

			w := postSettings(handler, url.Values{"week_start": {strconv.Itoa(int(day))}})

			if w.Code != http.StatusFound || w.Header().Get("Location") != "/settings?status=settings_saved" {
				t.Fatalf("UpdateSettings() = %d to %q, want %d to /settings?status=settings_saved", w.Code, w.Header().Get("Location"), http.StatusFound)
			}
			if got := users.users[100].WeekStart; got != day {
				t.Fatalf("saved week start = %v, want %v", got, day)
			}
		})
	}
}

func TestUpdateSettingsRefusesAnythingButAWeekday(t *testing.T) {
	for _, value := range []string{"", "7", "-1", "monday", "1.5", " 1"} {
		t.Run(value, func(t *testing.T) {
			users := &fakeUserRepo{users: map[int64]*store.User{100: {ID: 100, WeekStart: time.Tuesday}}}
			handler := NewHandler(&config.Config{}, &store.Store{Users: users}, nil, dav.ValidateCalendarObject, dav.EventInstances)

			w := postSettings(handler, url.Values{"week_start": {value}})

			if w.Code != http.StatusBadRequest {
				t.Fatalf("UpdateSettings(%q) status = %d, want %d", value, w.Code, http.StatusBadRequest)
			}
			if got := users.users[100].WeekStart; got != time.Tuesday {
				t.Fatalf("UpdateSettings(%q) changed week start to %v", value, got)
			}
		})
	}
}

func TestUpdateSettingsReportsStoreFailure(t *testing.T) {
	users := &fakeUserRepo{users: map[int64]*store.User{100: {ID: 100}}, setWeekStartErr: errors.New("boom")}
	handler := NewHandler(&config.Config{}, &store.Store{Users: users}, nil, dav.ValidateCalendarObject, dav.EventInstances)

	w := postSettings(handler, url.Values{"week_start": {"1"}})

	if w.Code != http.StatusInternalServerError {
		t.Fatalf("UpdateSettings() status = %d, want %d", w.Code, http.StatusInternalServerError)
	}
}

// Every page with a calendar grid hands it the user's first day of the week.
func TestCalendarGridPagesCarryUsersFirstDayOfWeek(t *testing.T) {
	handler := NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{calendars: map[int64]*store.Calendar{1: {ID: 1, UserID: 100, Name: "Work"}}},
		AddressBooks: &fakeAddressBookRepo{books: map[int64]*store.AddressBook{
			1: {ID: 1, UserID: 100, Name: "Contacts"},
		}},
		Contacts: &fakeContactRepoWithBirthdays{fakeContactRepo: fakeContactRepo{contacts: map[string]*store.Contact{}}},
	}, nil, dav.ValidateCalendarObject, dav.EventInstances)
	user := &store.User{ID: 100, PrimaryEmail: "owner@example.com", WeekStart: time.Wednesday}

	for _, page := range []struct {
		name   string
		serve  http.HandlerFunc
		path   string
		params map[string]string
	}{
		{"calendar", handler.ViewCalendar, "/calendars/1", map[string]string{"id": "1"}},
		{"all calendars", handler.ViewAllCalendars, "/calendars/all", nil},
		{"birthdays", handler.ViewBirthdays, "/birthdays", nil},
	} {
		t.Run(page.name, func(t *testing.T) {
			rctx := chi.NewRouteContext()
			for key, value := range page.params {
				rctx.URLParams.Add(key, value)
			}
			req := httptest.NewRequest(http.MethodGet, page.path, nil)
			req = req.WithContext(auth.WithUser(context.WithValue(req.Context(), chi.RouteCtxKey, rctx), user))
			w := httptest.NewRecorder()
			page.serve(w, req)

			if w.Code != http.StatusOK {
				t.Fatalf("status = %d, want %d", w.Code, http.StatusOK)
			}
			if !strings.Contains(w.Body.String(), "var WEEK_START = 3;") {
				t.Fatalf("page does not set WEEK_START to Wednesday")
			}
		})
	}
}

func postSettings(handler *Handler, form url.Values) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/settings", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req = req.WithContext(auth.WithUser(req.Context(), &store.User{ID: 100, PrimaryEmail: "user@example.com"}))
	w := httptest.NewRecorder()
	handler.UpdateSettings(w, req)
	return w
}
