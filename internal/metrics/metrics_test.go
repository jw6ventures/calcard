package metrics

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestMiddlewareRecordsRouteLabelsAndRequestID(t *testing.T) {
	httpRequestsTotal.Reset()
	httpErrorsTotal.Reset()
	httpRequestDuration.Reset()

	handler := middleware.RequestID(Middleware()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := RequestIDFromContext(r.Context()); got == "" || got != middleware.GetReqID(r.Context()) {
			t.Fatalf("RequestIDFromContext() = %q", got)
		}
		w.WriteHeader(http.StatusCreated)
	})))

	req := httptest.NewRequest(http.MethodGet, "/api/calendars/42", nil)
	rctx := chi.NewRouteContext()
	rctx.RoutePatterns = []string{"/api/calendars/{id}"}
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if got := testutil.ToFloat64(httpRequestsTotal.WithLabelValues(http.MethodGet, "/api/calendars/{id}")); got != 1 {
		t.Fatalf("request count = %v", got)
	}
	if got := testutil.ToFloat64(httpErrorsTotal.WithLabelValues(http.MethodGet, "/api/calendars/{id}", "201")); got != 0 {
		t.Fatalf("error count = %v", got)
	}
}

func TestMiddlewareCountsServerErrorsOnly(t *testing.T) {
	httpRequestsTotal.Reset()
	httpErrorsTotal.Reset()
	httpRequestDuration.Reset()

	handler := Middleware()(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))

	req := httptest.NewRequest(http.MethodPost, "/boom", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if got := testutil.ToFloat64(httpErrorsTotal.WithLabelValues(http.MethodPost, "/boom", "500")); got != 1 {
		t.Fatalf("error count = %v", got)
	}
}

func TestObserveDBLatencyUsesRouteFromContext(t *testing.T) {
	dbLatency.Reset()

	ObserveDBLatency(context.WithValue(context.Background(), routeLabelKey, "/readyz"), "db.healthcheck", time.Now().Add(-time.Second))
	if got := testutil.CollectAndCount(dbLatency); got == 0 {
		t.Fatal("expected dbLatency to collect samples")
	}
	if got := routeFromContext(context.Background()); got != "unknown" {
		t.Fatalf("routeFromContext() = %q", got)
	}
}

func TestRoutePatternFallsBackToPath(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/fallback", nil)
	if got := routePattern(req); got != "/fallback" {
		t.Fatalf("routePattern() = %q", got)
	}
}
