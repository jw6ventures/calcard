package store

import (
	"context"
	"time"

	"github.com/jw6ventures/calcard/internal/logging"
	"github.com/jw6ventures/calcard/internal/metrics"
)

// queryLogger is the package-level logger used to trace database queries. It is
// nil until SetLogger is called, at which point the *logging.Logger no-op
// behaviour still applies if the underlying sink is nil.
var queryLogger *logging.Logger

// SetLogger installs the sink used to trace database queries. It should be
// called once during startup, before the store handles traffic. A nil sink
// disables query logging.
func SetLogger(sink logging.Sink) {
	queryLogger = logging.New(sink, "Store")
}

// observeDB records query latency for metrics and emits trace/debug logs marking
// the start and completion of each query. The returned func must be deferred so
// it runs when the query completes.
func observeDB(ctx context.Context, operation string) func() {
	start := time.Now()
	queryLogger.Trace(operation, "executing query")
	return func() {
		metrics.ObserveDBLatency(ctx, operation, start)
		queryLogger.Trace(operation, "query completed in %s", time.Since(start))
	}
}
