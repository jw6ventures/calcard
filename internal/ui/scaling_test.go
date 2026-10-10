package ui

import (
	"math"
	"testing"
	"time"
)

// maxLinearScalingRatio separates linear work, which grows about fourfold from
// n to 4n, from quadratic work, which grows about sixteenfold.
const maxLinearScalingRatio = 12

// linearScalingFloor is the duration below which the larger run is too short
// for its ratio to mean anything.
const linearScalingFloor = 200 * time.Millisecond

func assertScalesLinearly(t *testing.T, n int, prepare func(n int) func()) {
	t.Helper()
	if raceDetectorEnabled {
		t.Skip("scaling is measured without the race detector, which multiplies every run")
	}
	if testing.Short() {
		t.Skip("scaling measurement skipped in -short mode")
	}
	small, large := prepare(n), prepare(4*n)
	smallBest, largeBest := time.Duration(math.MaxInt64), time.Duration(math.MaxInt64)
	for range 3 {
		started := time.Now()
		small()
		smallBest = min(smallBest, time.Since(started))
		started = time.Now()
		large()
		largeBest = min(largeBest, time.Since(started))
	}
	ratio := float64(largeBest) / float64(smallBest)
	t.Logf("n=%d took %v, 4n took %v: ratio %.1f", n, smallBest, largeBest, ratio)
	if largeBest >= linearScalingFloor && ratio > maxLinearScalingRatio {
		t.Errorf("work at 4n took %v against %v at n=%d, a ratio of %.1f; linear work stays under %d",
			largeBest, smallBest, n, ratio, maxLinearScalingRatio)
	}
}
