package store

import (
	"context"
	"time"

	"gitea.jw6.us/james/calcard/internal/metrics"
)

func observeDB(ctx context.Context, operation string) func() {
	start := time.Now()
	return func() {
		metrics.ObserveDBLatency(ctx, operation, start)
	}
}
