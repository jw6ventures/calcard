package vcard

import (
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

// A 2.1 export carries photos as base64 folded over thousands of lines and
// quoted-printable notes over many soft breaks; both unfold in linear time.
func TestConvert21To30ScalesLinearly(t *testing.T) {
	assertScalesLinearly(t, 3000, func(n int) func() {
		card := "BEGIN:VCARD\r\nVERSION:2.1\r\nFN:x\r\n" +
			"PHOTO;JPEG;ENCODING=BASE64:" + strings.Repeat("\r\n "+strings.Repeat("A", 72), n) + "\r\n\r\n" +
			"NOTE;ENCODING=QUOTED-PRINTABLE:" + strings.Repeat(strings.Repeat("b", 72)+"=\r\n", n) + "end\r\n" +
			"END:VCARD\r\n"
		return func() {
			if _, err := Convert21To30(card); err != nil {
				t.Fatal(err)
			}
		}
	})
}

func TestContentLinesScalesLinearly(t *testing.T) {
	assertScalesLinearly(t, 3000, func(n int) func() {
		card := "BEGIN:VCARD\r\nVERSION:3.0\r\nFN:x\r\nPHOTO;ENCODING=b:" + strings.Repeat(strings.Repeat("A", 74)+"\r\n ", n) + "A\r\nEND:VCARD\r\n"
		return func() {
			if lines := ContentLines(card); len(lines) != 5 {
				t.Fatalf("unfolded into %d lines", len(lines))
			}
		}
	})
}
