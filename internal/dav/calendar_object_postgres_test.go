package dav

import (
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jw6ventures/calcard/internal/auth"
	"github.com/jw6ventures/calcard/internal/store"
	_ "github.com/lib/pq"
)

func newDAVPostgresStore(t *testing.T) *store.Store {
	t.Helper()
	dsn := os.Getenv("CALCARD_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("CALCARD_TEST_POSTGRES_DSN is unset; set it to run the HTTP/PostgreSQL concurrency test")
	}

	schema := fmt.Sprintf("calcard_dav_test_%d", time.Now().UnixNano())
	admin, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open PostgreSQL: %v", err)
	}
	if err := admin.Ping(); err != nil {
		admin.Close()
		t.Fatalf("ping PostgreSQL: %v", err)
	}
	if _, err := admin.Exec(`CREATE SCHEMA ` + schema); err != nil {
		admin.Close()
		t.Fatalf("create schema %s: %v", schema, err)
	}

	pool, err := sql.Open("postgres", davPostgresSearchPath(t, dsn, schema))
	if err != nil {
		admin.Close()
		t.Fatalf("open PostgreSQL schema %s: %v", schema, err)
	}
	t.Cleanup(func() {
		pool.Close()
		if _, err := admin.Exec(`DROP SCHEMA ` + schema + ` CASCADE`); err != nil {
			t.Errorf("drop schema %s: %v", schema, err)
		}
		admin.Close()
	})

	schemaSQL, err := os.ReadFile(filepath.Join("..", "..", "db.sql"))
	if err != nil {
		t.Fatalf("read db.sql: %v", err)
	}
	if _, err := pool.Exec(string(schemaSQL)); err != nil {
		t.Fatalf("apply db.sql: %v", err)
	}
	return store.New(pool)
}

func davPostgresSearchPath(t *testing.T, dsn, schema string) string {
	t.Helper()
	if strings.HasPrefix(dsn, "postgres://") || strings.HasPrefix(dsn, "postgresql://") {
		parsed, err := url.Parse(dsn)
		if err != nil {
			t.Fatalf("parse CALCARD_TEST_POSTGRES_DSN: %v", err)
		}
		query := parsed.Query()
		query.Set("search_path", schema)
		parsed.RawQuery = query.Encode()
		return parsed.String()
	}
	return dsn + " search_path=" + schema
}

func TestPostgres_ConcurrentHTTPPutsOneUIDTwoResources(t *testing.T) {
	database := newDAVPostgresStore(t)
	ctx := t.Context()
	user, err := database.Users.UpsertOAuthUser(ctx, "dav-race", "dav-race@example.test", "DAV Race", "Test")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	calendar, err := database.Calendars.Create(ctx, store.Calendar{UserID: user.ID, Name: "Race"})
	if err != nil {
		t.Fatalf("create calendar: %v", err)
	}
	h := NewDavServer(Options{Store: database})

	const writers = 8
	type response struct {
		name string
		rr   *httptest.ResponseRecorder
	}
	responses := make([]response, writers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			name := fmt.Sprintf("resource-%d", i)
			body := buildCalendarObject(buildVEvent("shared-http-uid", fmt.Sprintf("SUMMARY:Writer %d", i)))
			req := newCalendarPutRequest(fmt.Sprintf("/dav/calendars/%d/%s.ics", calendar.ID, name), strings.NewReader(body))
			req = req.WithContext(auth.WithUser(req.Context(), user))
			rr := httptest.NewRecorder()
			h.Put(rr, req)
			responses[i] = response{name: name, rr: rr}
		}(i)
	}
	close(start)
	wg.Wait()

	created := ""
	for _, response := range responses {
		switch response.rr.Code {
		case http.StatusCreated:
			if created != "" {
				t.Fatalf("both %q and %q were created for one UID", created, response.name)
			}
			created = response.name
		case http.StatusConflict:
		default:
			t.Fatalf("PUT %q = %d, want 201 or 409: %s", response.name, response.rr.Code, response.rr.Body.String())
		}
	}
	if created == "" {
		t.Fatal("no competing HTTP PUT created the calendar object")
	}
	wantHref := fmt.Sprintf("/dav/calendars/%d/%s.ics", calendar.ID, created)
	for _, response := range responses {
		if response.name != created {
			assertUIDConflict(t, response.rr, wantHref)
		}
	}

	stored, err := database.Events.ListForCalendar(ctx, calendar.ID)
	if err != nil {
		t.Fatalf("list stored events: %v", err)
	}
	if len(stored) != 1 || stored[0].UID != "shared-http-uid" || stored[0].ResourceName != created {
		t.Fatalf("stored events = %+v, want only the winning HTTP resource", stored)
	}
}

func TestPostgres_ConcurrentHTTPPutIfNoneMatchCreatesOnce(t *testing.T) {
	database := newDAVPostgresStore(t)
	ctx := t.Context()
	user, err := database.Users.UpsertOAuthUser(ctx, "dav-if-none-match", "dav-if-none-match@example.test", "DAV If None Match", "Test")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	calendar, err := database.Calendars.Create(ctx, store.Calendar{UserID: user.ID, Name: "If None Match"})
	if err != nil {
		t.Fatalf("create calendar: %v", err)
	}
	h := NewDavServer(Options{Store: database})

	const writers = 8
	bodies := make([]string, writers)
	responses := make([]*httptest.ResponseRecorder, writers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			bodies[i] = buildCalendarObject(buildVEvent("if-none-match-http", fmt.Sprintf("SUMMARY:Writer %d", i)))
			req := newCalendarPutRequest(fmt.Sprintf("/dav/calendars/%d/contested.ics", calendar.ID), strings.NewReader(bodies[i]))
			req.Header.Set("If-None-Match", "*")
			req = req.WithContext(auth.WithUser(req.Context(), user))
			rr := httptest.NewRecorder()
			h.Put(rr, req)
			responses[i] = rr
		}(i)
	}
	close(start)
	wg.Wait()

	winner := -1
	for i, rr := range responses {
		switch rr.Code {
		case http.StatusCreated:
			if winner != -1 {
				t.Fatalf("writers %d and %d both created the If-None-Match target", winner, i)
			}
			winner = i
		case http.StatusPreconditionFailed:
		default:
			t.Fatalf("writer %d PUT = %d, want 201 or 412: %s", i, rr.Code, rr.Body.String())
		}
	}
	if winner == -1 {
		t.Fatal("no If-None-Match writer created the target")
	}

	stored, err := database.Events.GetByResourceName(ctx, calendar.ID, "contested")
	if err != nil || stored == nil {
		t.Fatalf("load winning event: %v", err)
	}
	if stored.RawICAL != bodies[winner] {
		t.Fatalf("stored body = %q, want winner %d body %q", stored.RawICAL, winner, bodies[winner])
	}
	if got, want := responses[winner].Header().Get("ETag"), `"`+stored.ETag+`"`; got != want {
		t.Fatalf("winning ETag = %q, want %q", got, want)
	}
}

func TestPostgres_ConcurrentHTTPPutIfMatchUpdatesOnce(t *testing.T) {
	database := newDAVPostgresStore(t)
	ctx := t.Context()
	user, err := database.Users.UpsertOAuthUser(ctx, "dav-if-match", "dav-if-match@example.test", "DAV If Match", "Test")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	calendar, err := database.Calendars.Create(ctx, store.Calendar{UserID: user.ID, Name: "If Match"})
	if err != nil {
		t.Fatalf("create calendar: %v", err)
	}
	h := NewDavServer(Options{Store: database})
	resourcePath := fmt.Sprintf("/dav/calendars/%d/contested.ics", calendar.ID)
	seedBody := buildCalendarObject(buildVEvent("if-match-http", "SUMMARY:Seed"))
	seedReq := newCalendarPutRequest(resourcePath, strings.NewReader(seedBody))
	seedReq = seedReq.WithContext(auth.WithUser(seedReq.Context(), user))
	seedRR := httptest.NewRecorder()
	h.Put(seedRR, seedReq)
	if seedRR.Code != http.StatusCreated {
		t.Fatalf("seed PUT = %d, want 201: %s", seedRR.Code, seedRR.Body.String())
	}
	seedETag := seedRR.Header().Get("ETag")
	if seedETag == "" {
		t.Fatal("seed PUT returned no ETag")
	}

	const writers = 8
	bodies := make([]string, writers)
	responses := make([]*httptest.ResponseRecorder, writers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			bodies[i] = buildCalendarObject(buildVEvent("if-match-http", fmt.Sprintf("SUMMARY:Writer %d", i)))
			req := newCalendarPutRequest(resourcePath, strings.NewReader(bodies[i]))
			req.Header.Set("If-Match", seedETag)
			req = req.WithContext(auth.WithUser(req.Context(), user))
			rr := httptest.NewRecorder()
			h.Put(rr, req)
			responses[i] = rr
		}(i)
	}
	close(start)
	wg.Wait()

	winner := -1
	for i, rr := range responses {
		switch rr.Code {
		case http.StatusNoContent:
			if winner != -1 {
				t.Fatalf("writers %d and %d both updated with the stale If-Match ETag", winner, i)
			}
			winner = i
		case http.StatusPreconditionFailed:
		default:
			t.Fatalf("writer %d PUT = %d, want 204 or 412: %s", i, rr.Code, rr.Body.String())
		}
	}
	if winner == -1 {
		t.Fatal("no If-Match writer updated the target")
	}

	stored, err := database.Events.GetByResourceName(ctx, calendar.ID, "contested")
	if err != nil || stored == nil {
		t.Fatalf("load winning event: %v", err)
	}
	if stored.RawICAL != bodies[winner] {
		t.Fatalf("stored body = %q, want winner %d body %q", stored.RawICAL, winner, bodies[winner])
	}
	if got, want := responses[winner].Header().Get("ETag"), `"`+stored.ETag+`"`; got != want {
		t.Fatalf("winning ETag = %q, want %q", got, want)
	}
}

func TestPostgres_ConcurrentHTTPMkcalendarCreatesOnce(t *testing.T) {
	database := newDAVPostgresStore(t)
	ctx := t.Context()
	user, err := database.Users.UpsertOAuthUser(ctx, "dav-mkcalendar", "dav-mkcalendar@example.test", "DAV MKCALENDAR", "Test")
	if err != nil {
		t.Fatalf("create user: %v", err)
	}
	h := NewDavServer(Options{Store: database})

	const writers = 8
	responses := make([]*httptest.ResponseRecorder, writers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < writers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			req := httptest.NewRequest("MKCALENDAR", "/dav/calendars/contested", nil)
			req = req.WithContext(auth.WithUser(req.Context(), user))
			rr := httptest.NewRecorder()
			h.Mkcalendar(rr, req)
			responses[i] = rr
		}(i)
	}
	close(start)
	wg.Wait()

	created := 0
	for i, rr := range responses {
		switch rr.Code {
		case http.StatusCreated:
			created++
		case http.StatusConflict:
			assertErrorConditions(t, rr, http.StatusConflict, davQN("resource-must-be-null"))
		default:
			t.Fatalf("MKCALENDAR writer %d = %d, want 201 or 409: %s", i, rr.Code, rr.Body.String())
		}
	}
	if created != 1 {
		t.Fatalf("successful MKCALENDAR requests = %d, want exactly 1", created)
	}

	calendars, err := database.Calendars.ListByUser(ctx, user.ID)
	if err != nil {
		t.Fatalf("list calendars: %v", err)
	}
	if len(calendars) != 1 || calendars[0].Slug == nil || *calendars[0].Slug != "contested" {
		t.Fatalf("stored calendars = %+v, want only slug contested", calendars)
	}
}
