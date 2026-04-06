package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/config"
	"github.com/jw6ventures/calcard/internal/events"
	"github.com/jw6ventures/calcard/internal/store"
)

type Handler struct {
	cfg    *config.Config
	store  *store.Store
	events *events.Service
}

func NewHandler(cfg *config.Config, st *store.Store) *Handler {
	return &Handler{
		cfg:    cfg,
		store:  st,
		events: events.NewService(st),
	}
}

type eventWriteRequest struct {
	InputMode  string                  `json:"inputMode"`
	RawICS     string                  `json:"rawIcal"`
	Structured *events.StructuredInput `json:"structured"`
}

type eventResponse struct {
	UID          string  `json:"uid"`
	CalendarID   int64   `json:"calendarId"`
	ResourceName string  `json:"resourceName"`
	Summary      *string `json:"summary,omitempty"`
	DTStart      *string `json:"dtstart,omitempty"`
	DTEnd        *string `json:"dtend,omitempty"`
	AllDay       bool    `json:"allDay"`
	ETag         string  `json:"etag"`
	LastModified string  `json:"lastModified"`
	RawICS       string  `json:"rawIcal"`
}

type calendarResponse struct {
	ID           int64                    `json:"id"`
	Name         string                   `json:"name"`
	Description  *string                  `json:"description,omitempty"`
	Timezone     *string                  `json:"timezone,omitempty"`
	Color        *string                  `json:"color,omitempty"`
	OwnerEmail   string                   `json:"ownerEmail"`
	Shared       bool                     `json:"shared"`
	Capabilities store.CalendarPrivileges `json:"capabilities"`
}

func calendarMetadataVisible(cal store.CalendarAccess) bool {
	if !cal.Shared {
		return true
	}
	return cal.EffectivePrivileges().HasAny()
}

func calendarResponseForAccess(cal store.CalendarAccess) calendarResponse {
	resp := calendarResponse{
		ID:           cal.ID,
		Shared:       cal.Shared,
		Capabilities: cal.EffectivePrivileges(),
	}
	if !calendarMetadataVisible(cal) {
		return resp
	}
	resp.Name = cal.Name
	resp.Description = cal.Description
	resp.Timezone = cal.Timezone
	resp.Color = cal.Color
	resp.OwnerEmail = cal.OwnerEmail
	return resp
}

func (h *Handler) ListCalendars(w http.ResponseWriter, r *http.Request) {
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}
	cals, err := h.events.ListCalendars(r.Context(), user)
	if err != nil {
		http.Error(w, "failed to load calendars", http.StatusInternalServerError)
		return
	}
	resp := make([]calendarResponse, 0, len(cals))
	for _, cal := range cals {
		resp = append(resp, calendarResponseForAccess(cal))
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) GetCalendar(w http.ResponseWriter, r *http.Request) {
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}
	calendarID, ok := parseCalendarID(w, r)
	if !ok {
		return
	}
	cal, err := h.events.GetCalendar(r.Context(), user, calendarID)
	if err != nil {
		writeEventError(w, err)
		return
	}
	writeJSON(w, http.StatusOK, calendarResponseForAccess(*cal))
}

func (h *Handler) ListEvents(w http.ResponseWriter, r *http.Request) {
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}
	calendarID, ok := parseCalendarID(w, r)
	if !ok {
		return
	}
	items, err := h.events.ListEvents(r.Context(), user, calendarID)
	if err != nil {
		writeEventError(w, err)
		return
	}
	resp := make([]eventResponse, 0, len(items))
	for _, ev := range items {
		resp = append(resp, toEventResponse(ev))
	}
	writeJSON(w, http.StatusOK, resp)
}

func (h *Handler) GetEvent(w http.ResponseWriter, r *http.Request) {
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}
	calendarID, uid, ok := parseCalendarIDAndUID(w, r)
	if !ok {
		return
	}
	ev, err := h.events.GetEvent(r.Context(), user, calendarID, uid)
	if err != nil {
		writeEventError(w, err)
		return
	}
	w.Header().Set("ETag", `"`+ev.ETag+`"`)
	writeJSON(w, http.StatusOK, toEventResponse(*ev))
}

func (h *Handler) CreateEvent(w http.ResponseWriter, r *http.Request) {
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}
	calendarID, ok := parseCalendarID(w, r)
	if !ok {
		return
	}
	input, err := decodeUpsertInput(r)
	if err != nil {
		writeEventError(w, err)
		return
	}
	ev, created, err := h.events.CreateEvent(r.Context(), user, calendarID, input)
	if err != nil {
		writeEventError(w, err)
		return
	}
	status := http.StatusOK
	if created {
		status = http.StatusCreated
	}
	w.Header().Set("ETag", `"`+ev.ETag+`"`)
	writeJSON(w, status, toEventResponse(*ev))
}

func (h *Handler) UpdateEvent(w http.ResponseWriter, r *http.Request) {
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}
	calendarID, uid, ok := parseCalendarIDAndUID(w, r)
	if !ok {
		return
	}
	input, err := decodeUpsertInput(r)
	if err != nil {
		writeEventError(w, err)
		return
	}
	ev, _, err := h.events.UpdateEvent(r.Context(), user, calendarID, uid, input)
	if err != nil {
		writeEventError(w, err)
		return
	}
	w.Header().Set("ETag", `"`+ev.ETag+`"`)
	writeJSON(w, http.StatusOK, toEventResponse(*ev))
}

func (h *Handler) DeleteEvent(w http.ResponseWriter, r *http.Request) {
	user, ok := auth.UserFromContext(r.Context())
	if !ok {
		http.Error(w, "missing user", http.StatusUnauthorized)
		return
	}
	calendarID, uid, ok := parseCalendarIDAndUID(w, r)
	if !ok {
		return
	}
	err := h.events.DeleteEvent(r.Context(), user, calendarID, uid, r.Header.Get("If-Match"), r.Header.Get("If-None-Match"))
	if err != nil {
		writeEventError(w, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func decodeUpsertInput(r *http.Request) (events.UpsertInput, error) {
	input := events.UpsertInput{
		ContentType: r.Header.Get("Content-Type"),
		IfMatch:     r.Header.Get("If-Match"),
		IfNoneMatch: r.Header.Get("If-None-Match"),
	}
	contentType := strings.ToLower(strings.TrimSpace(r.Header.Get("Content-Type")))
	if strings.HasPrefix(contentType, "text/calendar") || strings.HasPrefix(contentType, "application/ical") || strings.HasPrefix(contentType, "application/ics") {
		body, err := io.ReadAll(io.LimitReader(r.Body, events.MaxBodyBytes+1))
		if err != nil {
			return input, err
		}
		if int64(len(body)) > events.MaxBodyBytes {
			return input, fmtBadRequest(errors.New("request body too large"))
		}
		input.RawICS = string(body)
		return input, nil
	}

	body, err := io.ReadAll(io.LimitReader(r.Body, events.MaxBodyBytes+1))
	if err != nil {
		return input, err
	}
	if int64(len(body)) > events.MaxBodyBytes {
		return input, fmtBadRequest(errors.New("request body too large"))
	}
	if len(body) == 0 {
		return input, fmtBadRequest(errors.New("missing request body"))
	}
	var req eventWriteRequest
	dec := json.NewDecoder(strings.NewReader(string(body)))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&req); err != nil {
		return input, fmtBadRequest(err)
	}
	switch req.InputMode {
	case "", "structured":
		input.Structured = req.Structured
	case "raw_ical":
		input.RawICS = req.RawICS
	default:
		return input, fmtBadRequest(errors.New("invalid inputMode"))
	}
	if req.InputMode == "raw_ical" && strings.TrimSpace(input.RawICS) == "" {
		return input, fmtBadRequest(errors.New("rawIcal is required"))
	}
	if req.InputMode != "raw_ical" && req.Structured == nil {
		return input, fmtBadRequest(errors.New("structured is required"))
	}
	return input, nil
}

func parseCalendarID(w http.ResponseWriter, r *http.Request) (int64, bool) {
	calendarID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		http.Error(w, "invalid calendar id", http.StatusBadRequest)
		return 0, false
	}
	return calendarID, true
}

func parseCalendarIDAndUID(w http.ResponseWriter, r *http.Request) (int64, string, bool) {
	calendarID, ok := parseCalendarID(w, r)
	if !ok {
		return 0, "", false
	}
	rawUID := chi.URLParam(r, "uid")
	uid, err := url.PathUnescape(rawUID)
	if err != nil || uid == "" {
		uid = rawUID
	}
	if uid == "" {
		http.Error(w, "invalid event uid", http.StatusBadRequest)
		return 0, "", false
	}
	return calendarID, uid, true
}

func toEventResponse(ev store.Event) eventResponse {
	var dtstart, dtend *string
	if ev.DTStart != nil {
		v := ev.DTStart.UTC().Format(time.RFC3339)
		dtstart = &v
	}
	if ev.DTEnd != nil {
		v := ev.DTEnd.UTC().Format(time.RFC3339)
		dtend = &v
	}
	return eventResponse{
		UID:          ev.UID,
		CalendarID:   ev.CalendarID,
		ResourceName: ev.ResourceName,
		Summary:      ev.Summary,
		DTStart:      dtstart,
		DTEnd:        dtend,
		AllDay:       ev.AllDay,
		ETag:         ev.ETag,
		LastModified: ev.LastModified.UTC().Format(time.RFC3339),
		RawICS:       ev.RawICAL,
	}
}

func writeEventError(w http.ResponseWriter, err error) {
	status := events.StatusCode(err)
	if status == http.StatusInternalServerError {
		http.Error(w, "internal server error", status)
		return
	}
	http.Error(w, err.Error(), status)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func fmtBadRequest(err error) error {
	return errors.Join(events.ErrBadRequest, err)
}
