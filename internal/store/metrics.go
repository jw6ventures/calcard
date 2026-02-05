package store

import (
	"context"
	"time"

	"github.com/jw6ventures/calcard/internal/metrics"
)

func observeDB(ctx context.Context, operation string) func() {
	start := time.Now()
	return func() {
		metrics.ObserveDBLatency(ctx, operation, start)
	}
}
