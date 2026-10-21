package contacts

import (
	"context"
	"math"
	"strings"
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

// An imported card carrying a large folded photo is read in time linear in
// its size, whichever vCard version it is written in.
func TestImportScalesLinearlyWithCardSize(t *testing.T) {
	for _, version := range []string{"2.1", "3.0"} {
		t.Run(version, func(t *testing.T) {
			assertScalesLinearly(t, 3000, func(n int) func() {
				encoding := "ENCODING=b"
				if version == "2.1" {
					encoding = "ENCODING=BASE64"
				}
				card := "BEGIN:VCARD\r\nVERSION:" + version + "\r\nUID:big\r\nFN:Big\r\nPHOTO;" + encoding + ":" +
					strings.Repeat("\r\n "+strings.Repeat("A", 72), n) + "\r\n\r\nEND:VCARD\r\n"
				return func() {
					svc, _ := newTestService()
					result, err := svc.ImportVCards(context.Background(), owner, 1, card)
					if err != nil || result.Imported != 1 {
						t.Fatalf("ImportVCards = %+v, %v", result, err)
					}
				}
			})
		})
	}
}
