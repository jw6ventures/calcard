package metrics

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type ctxKey string

const (
	routeLabelKey   ctxKey = "metrics_route"
	requestIDCtxKey ctxKey = "metrics_request_id"
)

var (
	httpRequestsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "calcard_http_requests_total",
		Help: "Total number of HTTP requests processed.",
	}, []string{"method", "route"})

	httpErrorsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "calcard_http_errors_total",
		Help: "Total number of HTTP requests resulting in server errors.",
	}, []string{"method", "route", "status"})

	httpRequestDuration = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "calcard_http_request_duration_seconds",
		Help:    "Histogram of latencies for HTTP requests.",
		Buckets: prometheus.DefBuckets,
	}, []string{"method", "route", "status"})

	dbLatency = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "calcard_db_latency_seconds",
		Help:    "Histogram of database operation latencies.",
		Buckets: prometheus.DefBuckets,
	}, []string{"operation", "route"})
)

// Middleware records request metrics and enriches the context with labels for downstream instrumentation.
func Middleware() func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			route := routePattern(r)
			reqID := middleware.GetReqID(r.Context())

			ctx := context.WithValue(r.Context(), routeLabelKey, route)
			if reqID != "" {
				ctx = context.WithValue(ctx, requestIDCtxKey, reqID)
			}

			start := time.Now()
			ww := middleware.NewWrapResponseWriter(w, r.ProtoMajor)

			next.ServeHTTP(ww, r.WithContext(ctx))

			status := ww.Status()
			method := r.Method
			duration := time.Since(start).Seconds()
			statusCode := strconv.Itoa(status)

			httpRequestsTotal.WithLabelValues(method, route).Inc()
			httpRequestDuration.WithLabelValues(method, route, statusCode).Observe(duration)
			if status >= http.StatusInternalServerError {
				httpErrorsTotal.WithLabelValues(method, route, statusCode).Inc()
			}
		})
	}
}

// Handler exposes the Prometheus metrics endpoint.
func Handler() http.Handler {
	return promhttp.Handler()
}

// ObserveDBLatency records database latency for a given operation, associating it with request labels when available.
func ObserveDBLatency(ctx context.Context, operation string, start time.Time) {
	route := routeFromContext(ctx)
	dbLatency.WithLabelValues(operation, route).Observe(time.Since(start).Seconds())
}

// RequestIDFromContext extracts the request ID stored by the metrics middleware.
func RequestIDFromContext(ctx context.Context) string {
	if reqID, ok := ctx.Value(requestIDCtxKey).(string); ok {
		return reqID
	}
	return ""
}

func routeFromContext(ctx context.Context) string {
	if route, ok := ctx.Value(routeLabelKey).(string); ok && route != "" {
		return route
	}
	return "unknown"
}

func routePattern(r *http.Request) string {
	if rctx := chi.RouteContext(r.Context()); rctx != nil {
		if pattern := strings.TrimSpace(rctx.RoutePattern()); pattern != "" {
			return pattern
		}
	}
	return r.URL.Path
}
