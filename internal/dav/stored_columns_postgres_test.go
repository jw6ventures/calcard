package dav

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"

	"github.com/jw6ventures/calcard/internal/api"
	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/config"
)

// An event PUT in a zone only the object defines is stored with the instants
// its own VTIMEZONE names. Calendar-query matches it there, and the REST API,
// which reports the stored columns, says the same.
func TestPostgres_StoredColumnsOfAnEventInAZoneOnlyTheObjectDefines(t *testing.T) {
	h, database, user, calendars := postgresCalendarServer(t, 1)
	calendarID := calendars[0].ID
	collection := fmt.Sprintf("/dav/calendars/%d/", calendarID)
	if rr := requestAsUser(h, user, http.MethodPut, collection+"outlook.ics", outlookEasternEvent(), nil); rr.Code != http.StatusCreated {
		t.Fatalf("PUT = %d: %s", rr.Code, rr.Body.String())
	}

	stored, err := database.Events.GetByUID(t.Context(), calendarID, "outlook")
	if err != nil || stored == nil {
		t.Fatalf("read stored row: %v, %v", stored, err)
	}
	wantStart, wantEnd := utcInstant(t, "20240304T140000Z"), utcInstant(t, "20240304T150000Z")
	if stored.DTStart == nil || !stored.DTStart.Equal(wantStart) || stored.DTEnd == nil || !stored.DTEnd.Equal(wantEnd) {
		t.Errorf("stored dtstart, dtend = %v, %v; want %v, %v", stored.DTStart, stored.DTEnd, wantStart, wantEnd)
	}

	depth := map[string]string{"Depth": "1"}
	if rr := requestAsUser(h, user, "REPORT", collection, timeRangeQuery("20240304T143000Z", "20240304T144500Z"), depth); rr.Code != http.StatusMultiStatus || !strings.Contains(rr.Body.String(), "outlook.ics") {
		t.Errorf("calendar-query at the stored instants = %d without the event:\n%s", rr.Code, rr.Body.String())
	}

	rest := api.NewHandler(&config.Config{}, database, ValidateCalendarObject)
	req := httptest.NewRequest(http.MethodGet, "/api/calendars/"+strconv.FormatInt(calendarID, 10)+"/events/outlook", nil)
	route := chi.NewRouteContext()
	route.URLParams.Add("id", strconv.FormatInt(calendarID, 10))
	route.URLParams.Add("uid", "outlook")
	ctx := context.WithValue(auth.WithUser(req.Context(), user), chi.RouteCtxKey, route)
	rr := httptest.NewRecorder()
	rest.GetEvent(rr, req.WithContext(ctx))
	if rr.Code != http.StatusOK {
		t.Fatalf("REST GET = %d: %s", rr.Code, rr.Body.String())
	}
	var event struct {
		DTStart string `json:"dtstart"`
		DTEnd   string `json:"dtend"`
	}
	if err := json.Unmarshal(rr.Body.Bytes(), &event); err != nil {
		t.Fatalf("REST body: %v: %s", err, rr.Body.String())
	}
	if event.DTStart != "2024-03-04T14:00:00Z" || event.DTEnd != "2024-03-04T15:00:00Z" {
		t.Errorf("REST dtstart, dtend = %q, %q; want 2024-03-04T14:00:00Z, 2024-03-04T15:00:00Z", event.DTStart, event.DTEnd)
	}
}
