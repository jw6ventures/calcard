package ui

import (
	"bytes"
	"context"
	"errors"
	"log"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/dav"
	"github.com/jw6ventures/calcard/internal/store"
	"github.com/jw6ventures/calcard/internal/ui/utils"
)

// flashedError is the error the redirect of rr carries to the page, as the
// page shows it before it is capitalized.
func flashedError(t *testing.T, rr *httptest.ResponseRecorder) string {
	t.Helper()
	if rr.Code != http.StatusFound {
		t.Fatalf("status = %d, want a redirect carrying the error (%s)", rr.Code, rr.Body.String())
	}
	location, err := url.Parse(rr.Header().Get("Location"))
	if err != nil {
		t.Fatalf("Location %q: %v", rr.Header().Get("Location"), err)
	}
	return location.Query().Get("error")
}

const messagesEventBody = "BEGIN:VCALENDAR\r\nVERSION:2.0\r\nPRODID:-//Test//EN\r\nBEGIN:VEVENT\r\nUID:evt\r\nDTSTAMP:20260101T000000Z\r\n" +
	"DTSTART:20260115T100000Z\r\nDTEND:20260115T110000Z\r\nSUMMARY:Old\r\nEND:VEVENT\r\nEND:VCALENDAR\r\n"

// newMessagesHandler is a handler over an owner's calendar 1 holding the
// event evt, whose stored ETag is "tag".
func newMessagesHandler() *Handler {
	return NewHandler(&config.Config{}, &store.Store{
		Calendars: &fakeCalendarRepo{calendars: map[int64]*store.Calendar{1: {ID: 1, UserID: 100, Name: "Mine"}}},
		Events: &fakeEventRepoWithUpsert{fakeEventRepo: fakeEventRepo{events: map[string]*store.Event{
			"1:evt": {ID: 1, CalendarID: 1, UID: "evt", ResourceName: "evt", RawICAL: messagesEventBody, ETag: "tag"},
		}}},
	}, nil, dav.ValidateCalendarObject, dav.EventInstances)
}

func postEventForm(handle http.HandlerFunc, form url.Values) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/calendars/1/events/evt", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	routeContext := chi.NewRouteContext()
	routeContext.URLParams.Add("id", "1")
	routeContext.URLParams.Add("uid", "evt")
	req = req.WithContext(context.WithValue(auth.WithUser(req.Context(), &store.User{ID: 100}), chi.RouteCtxKey, routeContext))
	rr := httptest.NewRecorder()
	handle(rr, req)
	return rr
}

// A submission the event builder refuses is flashed in words that name the
// field and what is wrong with it, never as the builder's own error, which
// is written for the log.
func TestEventFormRefusalsAreFlashedInPlainWords(t *testing.T) {
	var logged bytes.Buffer
	log.SetOutput(&logged)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })

	valid := func() url.Values {
		return url.Values{
			"summary": {"Lunch"}, "dtstart": {"2026-01-15T10:00"}, "dtend": {"2026-01-15T11:00"}, "etag": {"tag"},
		}
	}
	with := func(pairs ...string) url.Values {
		form := valid()
		for i := 0; i < len(pairs); i += 2 {
			form.Set(pairs[i], pairs[i+1])
		}
		return form
	}
	tests := []struct {
		name   string
		form   url.Values
		want   string
		detail string
	}{
		{"start not a date", with("dtstart", "tomorrow at ten"), "The start date or time is not valid.", "invalid start date format"},
		{"end not a date", with("dtend", "2026-02-30T10:00"), "The end date or time is not valid.", "invalid end date format"},
		{"end before start", with("dtend", "2026-01-15T09:00"), "The end must not be before the start.", "end date must not be before start date"},
		{"days of the month", with("recurrence", "MONTHLY", "recurrence_bymonthday", "first"), "The days of the month must be numbers from 1 to 31 or -31 to -1, separated by commas.", `invalid day of month "first"`},
		{"status", with("status", "not valid"), "The status is not valid.", "invalid status"},
		{"visibility", with("class", "not valid"), "The visibility is not valid.", "invalid class"},
		{"availability", with("transparency", "not valid"), "The availability is not valid.", "invalid transp"},
		{"repeat frequency", with("recurrence", "SOMETIMES"), "The repeat frequency is not valid.", "invalid recurrence frequency"},
	}
	writes := []struct {
		name   string
		handle func(*Handler) http.HandlerFunc
	}{
		{"create", func(h *Handler) http.HandlerFunc { return h.CreateEvent }},
		{"edit", func(h *Handler) http.HandlerFunc { return h.UpdateEvent }},
	}
	for _, write := range writes {
		for _, tt := range tests {
			t.Run(write.name+": "+tt.name, func(t *testing.T) {
				logged.Reset()
				rr := postEventForm(write.handle(newMessagesHandler()), tt.form)
				if got := flashedError(t, rr); got != tt.want {
					t.Errorf("flashed error = %q, want %q", got, tt.want)
				}
				if !strings.Contains(logged.String(), tt.detail) {
					t.Errorf("the server log lacks the detail %q: %q", tt.detail, logged.String())
				}
			})
		}
	}
}

// An edit of one occurrence is refused in the same words.
func TestOccurrenceEditRefusalsAreFlashedInPlainWords(t *testing.T) {
	var logged bytes.Buffer
	log.SetOutput(&logged)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })

	rr := postEventForm(newMessagesHandler().UpdateEvent, url.Values{
		"summary": {"Lunch"}, "dtstart": {"2026-01-15T10:00"}, "dtend": {"2026-01-15T11:00"}, "etag": {"tag"},
		"edit_scope": {"occurrence"}, "recurrence_id": {"2026-01-15T10:00"}, "status": {"not valid"},
	})
	if got, want := flashedError(t, rr), "The status is not valid."; got != want {
		t.Errorf("flashed error = %q, want %q", got, want)
	}
	if !strings.Contains(logged.String(), "invalid status") {
		t.Errorf("the server log lacks the detail: %q", logged.String())
	}
}

// The builder's date errors carry the property they refused and the time
// package's own text, which no page shows.
func TestEventFormFlashNeverShowsTheParserText(t *testing.T) {
	var logged bytes.Buffer
	log.SetOutput(&logged)
	t.Cleanup(func() { log.SetOutput(os.Stderr) })

	_, parseErr := time.Parse("2006-01-02", "2026-13-01")
	tests := []struct {
		property string
		want     string
	}{
		{"DTSTART", "The start date or time is not valid."},
		{"DTEND", "The end date or time is not valid."},
		{"RECURRENCE-ID", "The occurrence date is not valid."},
		{"X-UNKNOWN", "The event could not be saved because some of its details are not valid."},
	}
	req := httptest.NewRequest(http.MethodPost, "/calendars/1/events", nil)
	for _, tt := range tests {
		logged.Reset()
		got := eventFormFlash(req, &utils.PropertyError{Name: tt.property, Err: parseErr})
		if got != tt.want {
			t.Errorf("%s: flash = %q, want %q", tt.property, got, tt.want)
		}
		if !strings.Contains(logged.String(), "parsing time") {
			t.Errorf("%s: the server log lacks the parser's text: %q", tt.property, logged.String())
		}
	}
	if got := eventFormFlash(req, errors.New("anything else")); got != "The event could not be saved because some of its details are not valid." {
		t.Errorf("unclassified refusal flash = %q", got)
	}
}
